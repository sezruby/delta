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

package org.apache.spark.sql.delta.commands

import java.sql.Timestamp

import org.apache.spark.sql.delta.{
  CatalogManagedTableMaintenanceOperation,
  DeltaColumnMapping,
  DeltaErrors,
  DeltaLog,
  SnapshotDescriptor}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{LeafCommand, LogicalPlan, UnaryCommand}

object DeltaReorgTableMode extends Enumeration {
  val PURGE, UNIFORM_ICEBERG, REWRITE_TYPE_WIDENING = Value
}

case class DeltaReorgTableSpec(
    reorgTableMode: DeltaReorgTableMode.Value,
    icebergCompatVersionOpt: Option[Int]
)

case class DeltaReorgTable(
    target: LogicalPlan,
    reorgTableSpec: DeltaReorgTableSpec = DeltaReorgTableSpec(DeltaReorgTableMode.PURGE, None))(
    val predicates: Seq[String]) extends UnaryCommand {

  def child: LogicalPlan = target

  protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(target = newChild)(predicates)

  override val otherCopyArgs: Seq[AnyRef] = predicates :: Nil
}

/**
 * The REORG TABLE command.
 *
 * @param applyPurgeFileSelection Whether APPLY (PURGE) honors the `reorg.purge.*` file selection
 *                                confs. Only set for user-issued REORG TABLE; internal callers
 *                                (e.g. dropping the deletionVectors feature) must purge all files.
 */
case class DeltaReorgTableCommand(
    target: LogicalPlan,
    reorgTableSpec: DeltaReorgTableSpec = DeltaReorgTableSpec(DeltaReorgTableMode.PURGE, None))(
    val predicates: Seq[String],
    val applyPurgeFileSelection: Boolean = false)
  extends OptimizeTableCommandBase
  with ReorgTableForUpgradeUniformHelper
  with LeafCommand {

  override val otherCopyArgs: Seq[AnyRef] =
    predicates :: Boolean.box(applyPurgeFileSelection) :: Nil

  override def optimizeByReorg(sparkSession: SparkSession): Seq[Row] = {
    val command = OptimizeTableCommand(
      target,
      predicates,
      optimizeContext = DeltaOptimizeContext(
        reorg = Some(reorgOperation),
        minFileSize = Some(0L),
        maxDeletedRowsRatio = Some(0d))
    )(zOrderBy = Nil)
    command.run(sparkSession)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = reorgTableSpec match {
    case DeltaReorgTableSpec(
        DeltaReorgTableMode.PURGE | DeltaReorgTableMode.REWRITE_TYPE_WIDENING, None) =>
      optimizeByReorg(sparkSession)
    case DeltaReorgTableSpec(DeltaReorgTableMode.UNIFORM_ICEBERG, Some(icebergCompatVersion)) =>
      val table = getDeltaTable(target, "REORG")
      DeltaErrors.checkCatalogManagedTableOperationAllowed(
        CatalogManagedTableMaintenanceOperation.DATA_REORGANIZATION,
        table.update(),
        table.catalogTable)
      upgradeUniformIcebergCompatVersion(table, sparkSession, icebergCompatVersion)
  }

  protected def reorgOperation: DeltaReorgOperation = reorgTableSpec match {
    case DeltaReorgTableSpec(DeltaReorgTableMode.PURGE, None) =>
      if (applyPurgeFileSelection) {
        new DeltaPurgeOperation(
          DeltaPurgeFileSelection.fromConf(getDeltaTable(target, "REORG").catalogTable))
      } else {
        new DeltaPurgeOperation()
      }
    case DeltaReorgTableSpec(DeltaReorgTableMode.UNIFORM_ICEBERG, Some(icebergCompatVersion)) =>
      new DeltaUpgradeUniformOperation(icebergCompatVersion)
    case DeltaReorgTableSpec(DeltaReorgTableMode.REWRITE_TYPE_WIDENING, None) =>
      new DeltaRewriteTypeWideningOperation()
  }
}

/**
 * Defines a Reorg operation to be applied during optimize.
 */
sealed trait DeltaReorgOperation {
  /**
   * Collects files that need to be processed by the reorg operation from the list of candidate
   * files.
   */
  def filterFilesToReorg(
      spark: SparkSession,
      snapshot: SnapshotDescriptor,
      files: Seq[AddFile]): Seq[AddFile]
}

/**
 * Reorg operation to purge files with soft deleted rows.
 * This operation will also try finding and removing the dropped columns from parquet files,
 * if ever exists such column that does not present in the current table schema.
 */
class DeltaPurgeOperation(fileSelection: Option[DeltaPurgeFileSelection] = None)
  extends DeltaReorgOperation with ReorgTableHelper {
  override def filterFilesToReorg(
      spark: SparkSession,
      snapshot: SnapshotDescriptor,
      files: Seq[AddFile]): Seq[AddFile] = {
    val physicalSchema = DeltaColumnMapping.renameColumns(snapshot.schema)
    val protocol = snapshot.protocol
    val metadata = snapshot.metadata
    val filesWithDroppedColumns: Seq[AddFile] =
      filterParquetFilesOnExecutors(spark, files, snapshot, ignoreCorruptFiles = false) {
        schema => fileHasExtraColumns(schema, physicalSchema, protocol, metadata)
      }
    val filesWithDV: Seq[AddFile] = files.filter { file =>
        (file.deletionVector != null && file.numPhysicalRecords.isEmpty) ||
        file.numDeletedRecords > 0L
    }
    val selectedFilesWithDV = fileSelection match {
      case Some(selection) => selection.select(snapshot, filesWithDV)
      case None => filesWithDV
    }
    (filesWithDroppedColumns ++ selectedFilesWithDV).distinct
  }
}

/**
 * Narrows the files with deletion vectors that REORG TABLE ... APPLY (PURGE) rewrites.
 *
 * A file is selected when
 *   (deleted-rows ratio >= minDeletedRowsRatio AND it got no DV within minStableDurationMs)
 *   OR it got no DV within maxDeletionVectorAgeMs.
 *
 * "Got a DV" means a data-changing commit (dataChange = true) added the file with a deletion
 * vector. Commit timestamps are resolved with [[DeltaHistoryManager.getActiveCommitAtTime]], so
 * in-commit timestamps are used when enabled.
 */
case class DeltaPurgeFileSelection(
    minDeletedRowsRatio: Double,
    minStableDurationMs: Long,
    maxDeletionVectorAgeMs: Option[Long],
    catalogTableOpt: Option[CatalogTable]) extends Logging {

  def isNoop: Boolean = minDeletedRowsRatio <= 0d && minStableDurationMs <= 0L

  def select(snapshot: SnapshotDescriptor, filesWithDV: Seq[AddFile]): Seq[AddFile] = {
    if (isNoop || filesWithDV.isEmpty) return filesWithDV
    val deltaLog = snapshot.deltaLog
    val now = deltaLog.clock.getTimeMillis()
    val stableWindow = if (minStableDurationMs > 0L) Some(minStableDurationMs) else None
    val windows = (stableWindow.toSeq ++ maxDeletionVectorAgeMs.toSeq).distinct
    val canonicalize = deltaLog.getCanonicalPathFunction(runsOnExecutors = false)
    val candidates = filesWithDV.map(f => f -> canonicalize(f.path))
    val recent = recentDeletionVectorPaths(
      deltaLog, snapshot.version, now, windows, candidates.map(_._2).toSet, canonicalize)

    // Unknown history: be conservative in each direction. Treat every file as recently changed
    // for the stability check, and as past the deadline for the age bound.
    def changedWithin(windowMs: Long, path: String, unknown: Boolean): Boolean =
      recent.get(windowMs) match {
        case Some(paths) => paths.contains(path)
        case None => unknown
      }

    val selected = candidates.filter { case (file, path) =>
      val ratioOk = deletedRowsRatio(file).forall(_ >= minDeletedRowsRatio)
      val stable = stableWindow.forall(w => !changedWithin(w, path, unknown = true))
      val pastDeadline =
        maxDeletionVectorAgeMs.exists(w => !changedWithin(w, path, unknown = false))
      (ratioOk && stable) || pastDeadline
    }.map(_._1)
    logInfo(log"REORG PURGE file selection for table ${MDC(DeltaLogKeys.PATH, deltaLog.dataPath)}" +
      log": selected ${MDC(DeltaLogKeys.NUM_FILES, selected.size.toLong)} of " +
      log"${MDC(DeltaLogKeys.NUM_FILES2, filesWithDV.size.toLong)} files with deletion vectors")
    selected
  }

  /** Deleted / physical rows, computed directly to avoid `1 - x` rounding at exact thresholds. */
  private def deletedRowsRatio(file: AddFile): Option[Double] =
    file.numPhysicalRecords.filter(_ > 0L).map(file.numDeletedRecords.toDouble / _)

  /**
   * For each window, the candidate paths that a data-changing commit added with a deletion vector
   * within that window, as of `snapshotVersion`. A window is missing from the result when the log
   * no longer covers its start. Only `candidatePaths` (canonicalized) are tracked, so driver
   * memory is bounded by the files being considered rather than by historical churn.
   */
  private def recentDeletionVectorPaths(
      deltaLog: DeltaLog,
      snapshotVersion: Long,
      now: Long,
      windows: Seq[Long],
      candidatePaths: Set[String],
      canonicalize: String => String): Map[Long, Set[String]] = {
    if (windows.isEmpty) return Map.empty
    val startVersions: Map[Long, Option[Long]] = windows.map { w =>
      val cutoff = now - w
      val commit = deltaLog.history.getActiveCommitAtTime(
        new Timestamp(cutoff),
        catalogTableOpt,
        canReturnLastCommit = true,
        mustBeRecreatable = false,
        canReturnEarliestCommit = true)
      val start = if (commit.timestamp <= cutoff) {
        Some(commit.version + 1)
      } else if (commit.version == 0L) {
        Some(0L)
      } else {
        // The earliest retained commit is already inside the window; older commits that may
        // also be inside it were cleaned up.
        logWarning(log"Cannot resolve deletion vector age for table " +
          log"${MDC(DeltaLogKeys.PATH, deltaLog.dataPath)}: the log does not cover the window")
        None
      }
      w -> start
    }.toMap

    val known = startVersions.collect { case (w, Some(v)) => w -> v }
    if (known.isEmpty) return Map.empty
    val firstVersion = known.values.min
    val addedAt = scala.collection.mutable.Map.empty[String, Long]
    if (firstVersion <= snapshotVersion) {
      deltaLog.getChanges(firstVersion, snapshotVersion, catalogTableOpt, failOnDataLoss = true)
        .foreach { case (version, actions) =>
          actions.foreach {
            case a: AddFile if a.dataChange && a.deletionVector != null =>
              val path = canonicalize(a.path)
              if (candidatePaths.contains(path)) addedAt(path) = version
            case _ =>
          }
        }
    }
    known.map { case (w, start) =>
      w -> addedAt.collect { case (path, v) if v >= start => path }.toSet
    }
  }
}

object DeltaPurgeFileSelection {
  /** Returns None when the confs leave every file with deletion vectors selected. */
  def fromConf(catalogTableOpt: Option[CatalogTable]): Option[DeltaPurgeFileSelection] = {
    val conf = org.apache.spark.sql.internal.SQLConf.get
    val selection = DeltaPurgeFileSelection(
      minDeletedRowsRatio = conf.getConf(DeltaSQLConf.DELTA_REORG_PURGE_MIN_DELETED_ROWS_RATIO),
      minStableDurationMs = conf.getConf(DeltaSQLConf.DELTA_REORG_PURGE_MIN_STABLE_DURATION),
      maxDeletionVectorAgeMs = conf.getConf(DeltaSQLConf.DELTA_REORG_PURGE_MAX_DELETION_VECTOR_AGE),
      catalogTableOpt = catalogTableOpt)
    if (selection.isNoop) None else Some(selection)
  }
}

/**
 * Reorg operation to upgrade the iceberg compatibility version of a table.
 */
class DeltaUpgradeUniformOperation(icebergCompatVersion: Int) extends DeltaReorgOperation {
  override def filterFilesToReorg(
      spark: SparkSession,
      snapshot: SnapshotDescriptor,
      files: Seq[AddFile]): Seq[AddFile] = {
    def shouldRewriteToBeIcebergCompatible(file: AddFile): Boolean = {
      if (file.tags == null) return true
      val fileIcebergCompatVersion =
        file.tags.getOrElse(AddFile.Tags.ICEBERG_COMPAT_VERSION.name, "0")
      fileIcebergCompatVersion != icebergCompatVersion.toString
    }
    files.filter(shouldRewriteToBeIcebergCompatible)
  }
}

/**
 * Internal reorg operation to rewrite files to conform to the current table schema when dropping
 * the type widening table feature.
 */
class DeltaRewriteTypeWideningOperation extends DeltaReorgOperation with ReorgTableHelper {
  override def filterFilesToReorg(
      spark: SparkSession,
      snapshot: SnapshotDescriptor,
      files: Seq[AddFile]): Seq[AddFile] = {
    val physicalSchema = DeltaColumnMapping.renameColumns(snapshot.schema)
    filterParquetFilesOnExecutors(spark, files, snapshot, ignoreCorruptFiles = false) {
      schema => fileHasDifferentTypes(schema, physicalSchema)
    }
  }
}
