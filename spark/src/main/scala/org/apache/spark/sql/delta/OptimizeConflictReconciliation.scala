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
   * Whether the reverse direction (DML loses to a compaction OPTIMIZE) is enabled. `protected` so
   * [[RowLevelConcurrencyResolution.canSkipAddedFileForRowLevelConcurrency]] (mixed into the same
   * checker) can also let the reverse remap's re-added outputs skip the append check.
   */
  protected lazy val optimizeReverseReconciliationEnabled: Boolean =
    spark.conf.get(DeltaSQLConf.DELTA_OPTIMIZE_CONFLICT_RECONCILIATION_REVERSE_ENABLED) &&
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

    recordTime("resolved-optimize-conflicts") {
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
    }
  }

  /**
   * Reverse direction of [[resolveOptimizeConflicts]]: the current transaction is a row-level DML
   * (DELETE/UPDATE) and the WINNING commit is a compaction OPTIMIZE that removed the source files
   * this DML added deletion vectors to. Instead of aborting, remap the DML's deletions onto the
   * winner's compacted output(s).
   *
   * The winner persisted, on each removed source's tombstone, that source's placement in the output
   * -- `(outputPath, outputStart, liveCount)`, read via
   * [[ConflictChecker.parseOptimizeSourceComposition]] from the `compactedInto` / `compactionInfo`
   * tags (a format modeled on what Databricks Runtime records; cross-engine reconciliation
   * against a DBR-written OPTIMIZE is best-effort, not a verified guarantee): removed
   * source `F`'s live rows landed contiguously at `[outputStart, outputStart + liveCount)`
   * of output `C`, in physical order. `F`'s deletion vector when the OPTIMIZE read it (`Do`, the
   * rows it excluded from `C`) is carried on that same winner `RemoveFile(F)`. A row `i` the DML
   * deletes lands at
   * `outputStart + liveRank_Do(i)`, where `liveRank_Do(i) = i - |Do below i|`; that position is
   * unioned into `C`'s (initially empty) deletion vector.
   *
   * Conservative and safe-by-abort: only when the winning commit is a PURE compaction (every
   * removed file is in some output's composition and every added file is a composition output),
   * only remapping the DML's NEW deletions (`loserDv \ loserReadDv`), only if every such row is
   * live in `C` (not in `Do`) and maps within the source's run; otherwise leave everything to the
   * standard checks (abort). A pure compaction is a `dataChange=false` relocation, so the DML's
   * read is invariant to it; every compaction source/output is marked resolved for the file checks.
   */
  protected def resolveReverseOptimizeConflicts(): Unit = {
    if (!optimizeReverseReconciliationEnabled) return

    // Every source the winner removed must carry a composition pointing at one of this commit's
    // outputs, and every added file must be a referenced compaction output -- i.e. the winning
    // commit is a PURE compaction. Otherwise we cannot safely rebase the DML onto it, so fall
    // through to abort. The composition is read via `parseOptimizeSourceComposition` from the
    // `compactedInto` / `compactionInfo` tags (a format modeled on Databricks Runtime's;
    // cross-engine reconciliation is best-effort); an unrecognized shape -> None (abort).
    val winnerRemoves = winningCommitSummary.removedFiles
    val winnerOutputs = winningCommitSummary.addedFiles
    if (winnerRemoves.isEmpty || winnerOutputs.isEmpty) return
    val winnerRemoveByPath = winnerRemoves.map(r => r.path -> r).toMap
    val outByPath = winnerOutputs.map(a => a.path -> a).toMap
    val outputPaths = outByPath.keySet

    // source path -> (output, liveCount, outputStart): the source's `liveCount` live rows land
    // contiguously at output positions [outputStart, outputStart + liveCount) in physical order,
    // read from the source tombstone's tag (the output AddFile is resolved by the recorded path).
    val srcToRun = mutable.Map.empty[String, (AddFile, Long, Long)]
    val referencedOutputs = mutable.Set.empty[String]
    var pureCompaction = true
    for (r <- winnerRemoves if pureCompaction) {
      ConflictChecker.parseOptimizeSourceComposition(r, outputPaths) match {
        case Some((outputPath, outputStart, liveCount)) =>
          srcToRun(r.path) = (outByPath(outputPath), liveCount, outputStart)
          referencedOutputs += outputPath
        case None =>
          // Absent/malformed tag, or a composition pointing outside this commit's outputs -> not a
          // pure, self-contained compaction we can rebase onto.
          pureCompaction = false
      }
    }
    // Pure compaction: every added output must be a referenced compaction target (no stray
    // appends), and a real compaction relocates rows (dataChange=false) -- a winner output claiming
    // dataChange is not the pure relocation the DML's read is invariant to. The dataChange guard is
    // belt-and-suspenders with the referenced-output check (a stray append would not be
    // referenced).
    if (!pureCompaction || referencedOutputs != outByPath.keySet ||
        winnerOutputs.exists(_.dataChange)) {
      return
    }

    // Current DML's DV updates: AddFile(F, dv) whose F the winner removed, plus their pre-image
    // RemoveFile(F) (whose DV is the DML's read-time DV on F).
    val currentAddByPath = currentTransactionInfo.actions.collect {
      case a: AddFile if a.deletionVector != null && winnerRemoveByPath.contains(a.path) =>
        a.path -> a
    }.toMap
    val currentRemoveByPath = currentTransactionInfo.actions.collect {
      case r: RemoveFile => r.path -> r
    }.toMap

    val sharedPaths = currentAddByPath.keySet.intersect(currentRemoveByPath.keySet)
    if (sharedPaths.isEmpty) return

    recordTime("resolved-reverse-optimize-conflicts") {
      val dvStore = DeletionVectorStore.createInstance(deltaLog.newDeltaHadoopConf())
      val tablePath = deltaLog.dataPath

      // Remapped DV accumulated per compacted output, seeded from the output's existing DV (a fresh
      // compaction output normally has none, but seed defensively so a pre-existing DV is preserved
      // rather than dropped when the output is tombstoned and re-added with the remapped DV).
      val outputDv = mutable.Map.empty[String, RoaringBitmapArray]
      val remappedSources = mutable.Set.empty[String]
      var allResolvable = true

      for (src <- sharedPaths if allResolvable) {
        // `Do`: F's DV when the winner OPTIMIZE read it. Rows in `Do` were excluded from `C`.
        val optimizeReadDv =
          readDeletionVectorOrEmpty(dvStore, winnerRemoveByPath(src).deletionVector, tablePath)
        // The DML's read-time DV on F and its final (post-delete) DV. Remap only the difference.
        val loserReadDv =
          readDeletionVectorOrEmpty(dvStore, currentRemoveByPath(src).deletionVector, tablePath)
        val loserDv =
          readDeletionVectorOrEmpty(dvStore, currentAddByPath(src).deletionVector, tablePath)
        val optimizeDeleted = optimizeReadDv.toArray
        val (out, liveCount, outputStart) = srcToRun(src)
        // Defense-in-depth (parity with the forward remap) against a corrupt or foreign composition
        // tag: the source's live run must fit within the output's physical record count. Out of
        // range is impossible for a real capture -> abort rather than remap out of bounds.
        if (out.numPhysicalRecords.exists(outputStart + liveCount > _)) {
          allResolvable = false
        }
        loserDv.forEach { i =>
          // The DML's DV is cumulative; remap only rows it newly deleted (vs its own read).
          if (!loserReadDv.contains(i)) {
            if (optimizeReadDv.contains(i)) {
              // The OPTIMIZE already excluded this row from `C` (it saw a delete the DML did not):
              // the compaction is not a consistent relocation of this row -> cannot remap.
              allResolvable = false
            } else {
              // Live-rank of physical row `i` = i minus the OPTIMIZE-read deletions below it; its
              // offset within the source's contiguous live-row run in the compacted output.
              val ins = java.util.Arrays.binarySearch(optimizeDeleted, i)
              val liveRank = i - (if (ins < 0) -(ins + 1) else ins)
              if (liveRank >= 0 && liveRank < liveCount) {
                val acc = outputDv.getOrElseUpdate(out.path,
                  readDeletionVectorOrEmpty(dvStore, out.deletionVector, tablePath).copy())
                acc.add(outputStart + liveRank)
              } else {
                allResolvable = false
              }
            }
          }
        }
        if (allResolvable) remappedSources += src
      }

      // H1: The pure-compaction resolution below blanket-marks EVERY winner-removed source resolved
      // for the file checks. But the remap loop only covers sources the DML re-added with a DV
      // (`sharedPaths` = removed AND re-added). A source the DML *fully* deleted -- a bare
      // `RemoveFile` with no re-added `AddFile` -- is winner-removed and DML-touched yet never
      // remapped; blanket-resolving it would silently drop that delete. Abort unless every
      // DML-touched winner-removed source was actually remapped.
      val dmlTouchedWinnerSources = winnerRemoveByPath.keySet
        .intersect(currentRemoveByPath.keySet.union(currentAddByPath.keySet))
      if (allResolvable && !dmlTouchedWinnerSources.subsetOf(remappedSources)) {
        allResolvable = false
      }

      // Reconcile only if every conflicting source was remappable; a partial remap could leave an
      // un-reconciled conflict, so otherwise leave everything to the standard checks (abort).
      if (allResolvable && outputDv.nonEmpty) {
        // Each affected output: tombstone the winner's (path, no-DV) file and re-add it with the
        // remapped DV (files are identified by path AND deletion vector). The composition lives on
        // the winner's source tombstones (never on this output), and the re-added output's own
        // tombstone would carry no composition tag, so a further concurrent loser aborts rather
        // than remapping onto an already-DV'd output. Nothing to strip here.
        val cRemoves = outputDv.keys.map(p => outByPath(p).removeWithTimestamp()).toSeq
        val cAdds = outputDv.map { case (p, bmp) =>
          val desc = writeMergedDeletionVector(dvStore, tablePath, bmp)
          outByPath(p).copy(deletionVector = desc, dataChange = true)
            .withoutTightBoundStats
        }.toSeq

        // Drop the DML's now-remapped AddFile/RemoveFile for the resolved sources; their deletions
        // now live on the compacted output instead.
        val newActions = currentTransactionInfo.actions.filterNot {
          case a: AddFile => remappedSources.contains(a.path)
          case r: RemoveFile => remappedSources.contains(r.path)
          case _ => false
        } ++ cRemoves ++ cAdds

        // A pure compaction is a dataChange=false relocation: mark ALL of its removed sources and
        // outputs resolved so neither the delete-read check (removed sources the DML read) nor the
        // append check (outputs, via canSkipAddedFileForRowLevelConcurrency) re-triggers.
        winnerRemoveByPath.keys.foreach(rowLevelResolvedPaths += _)
        winnerOutputs.foreach(rowLevelResolvedPaths += _.path)
        val newReadFiles = currentTransactionInfo.readFiles
          .filterNot(f => winnerRemoveByPath.contains(f.path))
        currentTransactionInfo =
          currentTransactionInfo.copy(actions = newActions, readFiles = newReadFiles)

        recordDeltaEvent(
          deltaLog,
          opType = "delta.optimize.conflictReconciliation.reverseRemapped",
          data = Map(
            "winningCommitVersion" -> winningCommitVersion,
            "remappedSources" -> remappedSources.size,
            "outputsRemapped" -> cAdds.size,
            "winningOperation" -> winningOperationName.getOrElse("UNKNOWN")))
      }
    }
  }
}
