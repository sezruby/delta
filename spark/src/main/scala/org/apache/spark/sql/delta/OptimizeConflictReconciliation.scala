/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore

/**
 * OPTIMIZE-vs-concurrent-DML reconciliation for the [[ConflictChecker]]: when a compaction OPTIMIZE
 * and a concurrent row-level DELETE/UPDATE touch the same source file, remap the DML's deletion
 * vector onto the compacted output (offset arithmetic) instead of aborting.
 *
 * Mixed into [[ConflictChecker]] as a self-typed trait alongside [[RowLevelConcurrencyResolution]],
 * whose `readDeletionVectorOrEmpty` / `writeMergedDeletionVector` / `rowLevelResolvedPaths` /
 * `winningOperationName` members it reuses (both traits share the same `ConflictChecker`
 * self-type). Lives in its own file to keep ConflictChecker focused on file-level conflict
 * detection. The static composition parser stays in `object ConflictChecker` as
 * [[ConflictChecker.parseOptimizeSourceComposition]].
 */
trait OptimizeConflictReconciliation extends DeltaLogging { self: ConflictChecker =>

  private lazy val optimizeReconciliationEnabled: Boolean =
    spark.conf.get(DeltaSQLConf.DELTA_OPTIMIZE_CONFLICT_RECONCILIATION_ENABLED) &&
      DeletionVectorUtils.deletionVectorsWritable(
        currentTransactionInfo.protocol, currentTransactionInfo.metadata)

  /**
   * Compaction OPTIMIZE vs a concurrent row-level DML. An OPTIMIZE that removed a source file `F`
   * and compacted it into an output `O` conflicts with a concurrent DELETE/UPDATE that added a
   * deletion vector to `F`. Instead of aborting, remap the concurrent DV onto `O`: the compaction
   * write recorded, on each removed source's tombstone, that the source's live rows landed at
   * output positions `[outputStart, outputStart + liveCount)` in physical order (the
   * `compactedInto` / `compactionInfo` composition tags; see [[RemoveFile.Tags.COMPACTION_INFO]]).
   * A deleted physical row `i` in `F` lands at `outputStart + liveRank(i)`, where `liveRank(i)` is
   * `i` minus the rows already deleted below it at read time; that mapped position is unioned into
   * `O`'s deletion vector.
   *
   * The read-time gaps are reconstructed here (from `F`'s read-time DV) rather than encoded in the
   * tag at write time, so a fragmented source DV cannot bloat the tag. Conservative by design: only
   * for tagged (compaction) sources, only if every newly-deleted row maps within the source's live
   * run, and only if every conflicting source is remappable; otherwise fall through to the standard
   * checks (abort). Only the winner's incremental (post-read) deletions are remapped. Handles the
   * direction where OPTIMIZE is the current (losing) transaction, reading the composition from its
   * own in-memory tombstones.
   */
  protected def resolveOptimizeConflicts(): Unit = {
    if (!optimizeReconciliationEnabled) return

    // Winning transaction's DV updates (a concurrent DELETE/UPDATE added a DV to a file).
    val winningRemovedPaths = winningCommitSummary.removedFiles.map(_.path).toSet
    val winningDvUpdates: Map[String, AddFile] = winningCommitSummary.addedFiles.iterator
      .filter(a => a.deletionVector != null && winningRemovedPaths.contains(a.path))
      .map(a => a.path -> a)
      .toMap
    if (winningDvUpdates.isEmpty) return

    // This OPTIMIZE's removed sources (some tagged with their composition) and its outputs by path.
    val currentRemoveByPath = currentTransactionInfo.actions.collect {
      case r: RemoveFile => r.path -> r
    }.toMap
    val currentAddByPath = currentTransactionInfo.actions.collect {
      case a: AddFile => a.path -> a
    }.toMap

    // source path -> (output, liveCount, outputStart): the source's `liveCount` live rows landed
    // contiguously at output positions [outputStart, outputStart + liveCount) in physical order,
    // read from the source tombstone's composition tags via the shared parser (the output AddFile
    // is resolved by the recorded path).
    val outputPaths = currentAddByPath.keySet
    val srcToRun = mutable.Map.empty[String, (AddFile, Long, Long)]
    for ((src, r) <- currentRemoveByPath) {
      ConflictChecker.parseOptimizeSourceComposition(r, outputPaths).foreach {
        case (outputPath, outputStart, liveCount) =>
          currentAddByPath.get(outputPath).foreach { out =>
            srcToRun(src) = (out, liveCount, outputStart)
          }
      }
    }
    if (srcToRun.isEmpty) return

    val sharedPaths = winningDvUpdates.keySet
      .intersect(srcToRun.keySet)
      .intersect(currentRemoveByPath.keySet)
    if (sharedPaths.isEmpty) return

    // Reconcile is a pure optimization over the conservative abort: mutate the transaction only
    // on the success path below, so any DV read/merge/write failure leaves it untouched and the
    // standard file-level checks abort cleanly rather than surfacing an unexpected error.
    try recordTime("resolved-optimize-conflicts") {
      val dvStore = DeletionVectorStore.createInstance(deltaLog.newDeltaHadoopConf())
      val tablePath = deltaLog.dataPath

      // Accumulate the remapped DV per output file, starting from its existing DV.
      val outputDv = mutable.Map.empty[String, RoaringBitmapArray]
      val resolvedSources = mutable.Set.empty[String]
      var allResolvable = true

      for (src <- sharedPaths if allResolvable) {
        // `F`'s deletion vector when OPTIMIZE read it. If non-empty, those rows were already gone
        // from the compacted output, so the recorded run covers only `F`'s live rows and a deleted
        // physical row's output offset must be discounted by the read-time deletions below it.
        val readTimeDv =
          readDeletionVectorOrEmpty(dvStore, currentRemoveByPath(src).deletionVector, tablePath)
        val winnerDv =
          readDeletionVectorOrEmpty(dvStore, winningDvUpdates(src).deletionVector, tablePath)
        // Sorted ascending, so the count of read-time deletions below a physical row is a binary
        // search. This is the read-time gap reconstruction deferred from write time to here.
        val readTimeDeleted = readTimeDv.toArray
        val (out, liveCount, outputStart) = srcToRun(src)
        // Defense-in-depth against a corrupt or foreign composition tag: the source's live run must
        // fit within the output file's physical record count. An out-of-range range is impossible
        // for a real capture, so treat it as unreconcilable (abort) rather than remap out of
        // bounds. Cheap: reads the already-parsed stats; a no-op on the happy path.
        if (out.numPhysicalRecords.exists(outputStart + liveCount > _)) {
          allResolvable = false
        }
        winnerDv.forEach { i =>
          // The winner's DV is cumulative. Rows already deleted at read time are not in the output,
          // so remap only the winner's NEW deletions; each lands among the source's live rows.
          if (!readTimeDv.contains(i)) {
            // Live-rank of physical row `i` = i minus the read-time deletions below it; that is
            // its offset within the source's contiguous live-row run in the compacted output.
            val ins = java.util.Arrays.binarySearch(readTimeDeleted, i)
            val liveRank = i - (if (ins < 0) -(ins + 1) else ins)
            if (liveRank >= 0 && liveRank < liveCount) {
              val acc = outputDv.getOrElseUpdate(out.path,
                readDeletionVectorOrEmpty(dvStore, out.deletionVector, tablePath).copy())
              acc.add(outputStart + liveRank)
            } else {
              // A newly-deleted physical row does not map within the source's live run -> cannot
              // remap.
              allResolvable = false
            }
          }
        }
        if (allResolvable) resolvedSources += src
      }

      // Reconcile only if every conflicting source was remappable; a partial remap could leave an
      // un-reconciled conflict, so otherwise leave everything to the standard checks (abort).
      if (allResolvable && outputDv.nonEmpty) {
        // Each compacted output gets the remapped/unioned DV.
        val addReplacements: Map[String, AddFile] = outputDv.keys.iterator
          .map { p =>
            // Writes a new DV file as a side effect of conflict resolution. If this commit later
            // aborts or retries against another winner, the file is unreferenced and reclaimed by
            // VACUUM (same lifecycle as any DV the DML write path persists). See the note on
            // `writeMergedDeletionVector`.
            val desc = writeMergedDeletionVector(dvStore, tablePath, outputDv(p))
            p -> currentAddByPath(p).copy(deletionVector = desc).withoutTightBoundStats
          }.toMap
        // Re-point each resolved source's RemoveFile at the winner's post-image (path + winning
        // DV). The OPTIMIZE read the pre-winner version, so its (path, no-DV) RemoveFile would not
        // match the winner's now-live (path, DV) file and would leave it undeleted (files are
        // identified by path AND deletion vector).
        val removeReplacements: Map[String, RemoveFile] =
          resolvedSources.iterator.map(src =>
            // dataChange = false: an OPTIMIZE commit is a data-preserving relocation, so the
            // reconciled source tombstone must match the dataChange=false OPTIMIZE output (keeps
            // streaming/CDC transparent and avoids mixing dataChange values in one commit).
            src -> winningDvUpdates(src).removeWithTimestamp(dataChange = false)).toMap

        val newActions = currentTransactionInfo.actions.map {
          case a: AddFile if addReplacements.contains(a.path) => addReplacements(a.path)
          case r: RemoveFile if removeReplacements.contains(r.path) => removeReplacements(r.path)
          case other => other
        }
        // The reconciled sources are no longer read/delete conflicts for the standard checks.
        resolvedSources.foreach(rowLevelResolvedPaths += _)
        val newReadFiles = currentTransactionInfo.readFiles
          .filterNot(f => resolvedSources.contains(f.path))
        currentTransactionInfo =
          currentTransactionInfo.copy(actions = newActions, readFiles = newReadFiles)

        recordDeltaEvent(
          deltaLog,
          opType = "delta.optimize.conflictReconciliation.remapped",
          data = Map(
            "winningCommitVersion" -> winningCommitVersion,
            "resolvedSources" -> resolvedSources.size,
            "outputsRemapped" -> addReplacements.size,
            "winningOperation" -> winningOperationName.getOrElse("UNKNOWN")))
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"OPTIMIZE-vs-DML conflict reconciliation failed; leaving all conflicts " +
          log"for the standard checks to arbitrate", e)
    }
  }
}
