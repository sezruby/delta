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

import java.util.concurrent.Future

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.threads.DeltaThreadPool

import org.apache.spark.util.ThreadUtils

/**
 * Checker-side glue for the delete/read refinement of conflict detection -- the counterpart to the
 * reader-side [[org.apache.spark.sql.delta.stats.ConflictDataSkippingReader]]. Mixed into
 * [[ConflictChecker]] as a self-typed trait so the feature stays a single isolated, additive unit:
 * it only ADDS members, and the checker's existing conflict logic calls into them behind the
 * value-exact conflict-skipping flags.
 *
 * The refinement replaces the path-keyed delete/read abort with a row-level one -- a concurrently
 * removed file the transaction read conflicts only when a removed row actually matches what the
 * transaction read. The scan can run as its own Spark job concurrently with the added-files scan;
 * see [[maybeStartDeleteReadRowScan]] and [[deleteReadRowMatches]].
 */
trait ConflictCheckerDataSkipping { self: ConflictChecker =>

  /**
   * Whether the delete/read refinement is active. It reads data during conflict detection, so it
   * rides on the value-exact added-files skipping flags rather than a separate config: when on, a
   * merge-on-read file the winner removed and re-added at the same path is excluded from the
   * added-files check (it carries no new rows), and the delete/read check aborts only when a row
   * the winner actually removed matches what the transaction read.
   */
  protected def deleteReadRowLevelRefinementEnabled: Boolean =
    spark.conf.get(DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_ENABLED) &&
      spark.conf.get(DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_VALUE_EXACT_ENABLED)

  /** The winner-removed files the current transaction read (the delete/read overlap set). */
  protected def readOverlappingRemovedFiles: Seq[RemoveFile] = {
    val readFilePaths = currentTransactionInfo.readFiles.map(_.path).toSet
    winningCommitSummary.removedFiles.filter(r => readFilePaths.contains(r.path))
  }

  /**
   * True iff the winner removed at least one row matching a read predicate from
   * `overlappingRemovedFiles` (the removed files the current transaction read). Rows are recovered
   * without an inverse deletion-vector read: because a DML only ever ADDS deletions, the winner's
   * new DV is a superset of the pre-image one, so matches(removed) = matches(pre-image live) -
   * matches(post-image live), where the pre-image is the removed file under its old DV and the
   * post-image is the winner's paired re-add under its new DV (absent for a full-file removal).
   * Conservative and fail-safe: a partitioned removed file with no recorded partition values
   * (unreadable) returns true, and any scan error inside
   * [[ConflictDataSkippingReader.anyRemovedRowMatchesReadPredicate]] returns true.
   */
  private def removedRowMatchesReadPredicate(
      overlappingRemovedFiles: Seq[RemoveFile]): Boolean = {
    val readSnapshot = currentTransactionInfo.readSnapshot
    if (readSnapshot.metadata.partitionColumns.nonEmpty &&
        overlappingRemovedFiles.exists(_.partitionValues == null)) {
      return true
    }
    val preImage = overlappingRemovedFiles.map(r =>
      AddFile(
        path = r.path,
        partitionValues = Option(r.partitionValues).getOrElse(Map.empty),
        size = r.size.getOrElse(0L),
        modificationTime = 0L,
        dataChange = false,
        stats = r.stats,
        tags = r.tags,
        deletionVector = r.deletionVector))
    val postImage = overlappingRemovedFiles
      .flatMap(r => winningCommitSummary.addedFilePathToActionMap.get(r.path))
    readSnapshot.anyRemovedRowMatchesReadPredicate(
      preImage, postImage, currentTransactionInfo.readPredicates.map(_.dataPredicates).toSeq)
  }

  // In-flight delete/read scan, harvested once by the delete/read check. Started before the
  // added-files scan so the two jobs overlap. Driver-thread only.
  private var deleteReadRowScan: Option[Future[Boolean]] = None

  /**
   * Start the delete/read scan as its own Spark job -- under the exact conditions the delete/read
   * check would run it -- so it overlaps the added-files scan on the checker thread. Two
   * independent queries, no fusion.
   */
  protected def maybeStartDeleteReadRowScan(): Unit = {
    if (!deleteReadRowLevelRefinementEnabled ||
        currentTransactionInfo.readWholeTable ||
        currentTransactionInfo.readPredicates.isEmpty) {
      return
    }
    val overlapping = readOverlappingRemovedFiles
    if (overlapping.nonEmpty) {
      deleteReadRowScan = Some(
        ConflictCheckerDataSkipping.rowLevelScanThreadPool.submit(spark) {
          removedRowMatchesReadPredicate(overlapping)
        })
    }
  }

  /** Harvest the concurrent scan if started, else run inline (fail-safe on failure). */
  protected def deleteReadRowMatches(overlappingRemovedFiles: Seq[RemoveFile]): Boolean = {
    deleteReadRowScan match {
      case Some(future) =>
        try ThreadUtils.awaitResult(future, Duration.Inf)
        catch { case NonFatal(_) => true }
      case None => removedRowMatchesReadPredicate(overlappingRemovedFiles)
    }
  }
}

object ConflictCheckerDataSkipping {
  // Pool for the delete/read conflict scan, run concurrently with the added-files scan.
  // submit(spark) forwards the caller's session + Spark local properties to the pool thread.
  private lazy val rowLevelScanThreadPool: DeltaThreadPool =
    DeltaThreadPool("delta-conflict-row-scan", 8)
}
