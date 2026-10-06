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

package org.apache.spark.sql.delta.commands.optimize

import scala.annotation.tailrec

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaConfigs, DeltaErrors, Snapshot}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.stats.DeltaStatistics.{MAX, MIN}
import org.apache.spark.sql.delta.util.DeltaSqlParserUtils

import org.apache.spark.sql.{Column, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

/**
 * Orders the files selected for compaction by the min/max statistics of the columns listed in
 * [[DeltaConfigs.COMPACTION_BINNING_COLUMNS]].
 *
 * Compaction packs files into bins sequentially in the order it is given, and each bin is written
 * out as one file. By default files are ordered by size, so a bin can mix files from anywhere in
 * the value range of every column and the output files lose the narrow min/max ranges that the
 * input files had. Ordering the files by (min, max) of the binning columns instead makes each bin
 * cover a contiguous range of values, so file-level data skipping on these columns keeps working
 * after compaction. Only the grouping of files changes; rows are still not reordered.
 *
 * Files without statistics for the binning columns are placed last, ordered by size.
 *
 * @param columns The binning columns, each given as its logical name parts.
 */
case class StatsBasedFileOrdering(
    spark: SparkSession,
    snapshot: Snapshot,
    columns: Seq[Seq[String]]) {

  private var sortKeys: Map[String, Array[Any]] = Map.empty

  /** Loads the min/max values of the binning columns for the given files. */
  def loadSortKeys(files: Seq[AddFile]): Unit = {
    if (files.isEmpty) return
    val keyColumns: Seq[Column] = columns.flatMap { nameParts =>
      Seq(MIN, MAX).map(snapshot.getStatsColumnOrNullLiteral(_, nameParts.reverse))
    }
    val paths = spark.createDataset(files.map(_.path).distinct)(Encoders.STRING).toDF("path")
    val rows = snapshot.withStats
      .join(paths, "path")
      .select(col("path") +: keyColumns: _*)
      .collect()
    sortKeys = rows.map { row =>
      row.getString(0) -> Array.tabulate[Any](keyColumns.size)(i => row.get(i + 1))
    }.toMap
  }

  /** Returns the files ordered by the min/max values of the binning columns. */
  def sort(files: Seq[AddFile]): Seq[AddFile] = {
    val noKeys = Array.empty[Any]
    files
      .map(f => (sortKeys.getOrElse(f.path, noKeys), f))
      .sortWith { case ((k1, f1), (k2, f2)) =>
        val c = StatsBasedFileOrdering.compareKeys(k1, k2)
        if (c != 0) c < 0 else if (f1.size != f2.size) f1.size < f2.size else f1.path < f2.path
      }
      .map(_._2)
  }
}

object StatsBasedFileOrdering {

  /**
   * Returns the ordering configured by [[DeltaConfigs.COMPACTION_BINNING_COLUMNS]] for the
   * table, or None if no binning columns are configured.
   */
  def fromSnapshot(spark: SparkSession, snapshot: Snapshot): Option[StatsBasedFileOrdering] = {
    DeltaConfigs.COMPACTION_BINNING_COLUMNS.fromMetaData(snapshot.metadata)
      .flatMap(DeltaSqlParserUtils.parseMultipartColumnList)
      .filter(_.nonEmpty)
      .map(attrs => StatsBasedFileOrdering(spark, snapshot, attrs.map(validate(snapshot, _))))
  }

  /** Checks that the column exists, is not a partition column and has min/max statistics. */
  private def validate(snapshot: Snapshot, column: UnresolvedAttribute): Seq[String] = {
    val nameParts = column.nameParts
    val isPartitionColumn = nameParts.size == 1 &&
      snapshot.metadata.partitionColumns.exists(_.equalsIgnoreCase(nameParts.head))
    if (isPartitionColumn) {
      throw DeltaErrors.invalidCompactionBinningColumn(
        column.name, "files are already grouped by partition columns")
    }
    if (SchemaUtils.findNestedFieldIgnoreCase(snapshot.schema, nameParts).isEmpty) {
      throw DeltaErrors.columnNotInSchemaException(column.name, snapshot.schema)
    }
    if (!hasMinMaxStats(snapshot, nameParts)) {
      throw DeltaErrors.invalidCompactionBinningColumn(
        column.name,
        "the column has no min/max statistics. Statistics are only collected for the columns " +
          s"in '${DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.key}' or the first " +
          s"'${DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.key}' columns, and only for " +
          "non-nested types that support min/max")
    }
    nameParts
  }

  private def hasMinMaxStats(snapshot: Snapshot, nameParts: Seq[String]): Boolean = {
    // Walks the table schema and the minValues stats schema in parallel. The stats schema uses
    // physical names, so each step resolves the logical name in the table schema first.
    @tailrec
    def hasStats(table: StructType, stats: StructType, parts: Seq[String]): Boolean = {
      val fieldOpt = table.find(_.name.equalsIgnoreCase(parts.head))
      val statsFieldOpt =
        fieldOpt.flatMap(f => stats.find(_.name == DeltaColumnMapping.getPhysicalName(f)))
      (fieldOpt.map(_.dataType), statsFieldOpt.map(_.dataType), parts.tail) match {
        case (Some(t: StructType), Some(s: StructType), rest) if rest.nonEmpty =>
          hasStats(t, s, rest)
        case (Some(_), Some(s), rest) => rest.isEmpty && !s.isInstanceOf[StructType]
        case _ => false
      }
    }
    snapshot.statsSchema.find(_.name == MIN).map(_.dataType) match {
      case Some(minValues: StructType) => hasStats(snapshot.schema, minValues, nameParts)
      case _ => false
    }
  }

  /** Compares two sort keys element by element; null values sort after all other values. */
  private[delta] def compareKeys(k1: Array[Any], k2: Array[Any]): Int = {
    if (k1.isEmpty || k2.isEmpty) return java.lang.Boolean.compare(k1.isEmpty, k2.isEmpty)
    var i = 0
    while (i < k1.length && i < k2.length) {
      val c = (k1(i), k2(i)) match {
        case (null, null) => 0
        case (null, _) => 1
        case (_, null) => -1
        case (a, b) => a.asInstanceOf[Comparable[Any]].compareTo(b)
      }
      if (c != 0) return c
      i += 1
    }
    0
  }
}
