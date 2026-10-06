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

import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterBySpec
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{Action, AddFile, FileAction}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaStatistics
import org.apache.spark.sql.delta.util.{DeltaFileOperations, JsonUtils}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{DecimalNode, LongNode, ObjectNode, TextNode}
import org.apache.parquet.format.converter.ParquetMetadataConverter

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.parquet.ParquetFooterReaderShims
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/**
 * Command that replaces the partitioning of a Delta table with clustering:
 *  - ALTER TABLE .. REPLACE PARTITIONED BY WITH CLUSTER BY (col1, col2, ...)
 *  - ALTER TABLE .. REPLACE PARTITIONED BY WITH CLUSTER BY NONE
 *
 * After the conversion the former partition columns are read from the data files, so they must be
 * physically stored in every data file (see `delta.writePartitionColumnsToParquet` and the
 * `materializePartitionColumns` table feature). The command reads the Parquet footers of all data
 * files to find the files that do not store them.
 *
 * The conversion is a single commit which
 *  1. clears the partition columns in the table metadata,
 *  2. re-adds every file that stores the partition columns with empty `partitionValues` and
 *     `dataChange = false`, keeping its path, deletion vector and row tracking fields. Per-file
 *     min/max/nullCount statistics are synthesized for the former partition columns from the
 *     (constant) partition values so that data skipping on these columns keeps working,
 *  3. rewrites every file that does not store the partition columns (if any), like OPTIMIZE does:
 *     the new files store all columns, deletion vectors are applied and row IDs and row commit
 *     versions are preserved, and
 *  4. when clustering columns are given, enables clustering and records the clustering columns.
 *
 * If all files store the partition columns, the conversion does not rewrite any data.
 *
 * The commit is done with [[OptimisticTransaction.commitLarge]], which fails if any other commit
 * happened since the transaction started. Hence no file can be added concurrently between the
 * verification and the commit.
 */
case class AlterTableReplacePartitionedByWithClusterByDeltaCommand(
    table: DeltaTableV2,
    clusteringColumns: Seq[Seq[String]])
  extends LeafRunnableCommand with AlterDeltaTableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.alter.replacePartitionedByWithClusterBy") {
      val txn = startTransaction()
      val snapshot = txn.snapshot
      val oldMetadata = txn.metadata
      if (oldMetadata.partitionColumns.isEmpty) {
        throw DeltaErrors.replacePartitionedByOnUnpartitionedTableException()
      }
      ClusteredTableUtils.validateNumClusteringColumns(clusteringColumns, Some(deltaLog))

      val physicalPartitionSchema = oldMetadata.physicalPartitionSchema
      val conf = sparkSession.sessionState.conf
      val filesToRewrite = if (conf.getConf(
          DeltaSQLConf.DELTA_REPLACE_PARTITIONED_BY_VERIFY_MATERIALIZED_PARTITION_COLUMNS)) {
        ReplacePartitionedByUtils.findFilesWithoutPartitionColumns(
          sparkSession, deltaLog, snapshot, physicalPartitionSchema.fieldNames.toSeq)
      } else {
        Seq.empty
      }
      if (filesToRewrite.nonEmpty && !conf.getConf(
          DeltaSQLConf.DELTA_REPLACE_PARTITIONED_BY_REWRITE_NON_MATERIALIZED_FILES)) {
        throw DeltaErrors.replacePartitionedByPartitionColumnsNotMaterializedException(
          oldMetadata.partitionColumns,
          filesToRewrite.size,
          filesToRewrite.take(ReplacePartitionedByUtils.NUM_EXAMPLE_FILES).map(_.path))
      }

      val newLogicalClusteringColumns = clusteringColumns.map(FieldReference(_).toString)
      val baseConfiguration = oldMetadata.configuration
      val newConfiguration =
        if (clusteringColumns.nonEmpty && !ClusteredTableUtils.isSupported(txn.protocol)) {
          baseConfiguration ++ ClusteredTableUtils.getTableFeatureProperties(baseConfiguration)
        } else {
          baseConfiguration
        }
      txn.updateMetadata(oldMetadata.copy(partitionColumns = Nil, configuration = newConfiguration))

      val domainMetadata = if (clusteringColumns.nonEmpty) {
        ClusteredTableUtils.validateClusteringColumnsInStatsSchema(
          txn.protocol, txn.metadata, ClusterBySpec(clusteringColumns))
        ClusteredTableUtils.getClusteringDomainMetadataForAlterTableClusterBy(
          newLogicalClusteringColumns, txn)
      } else {
        Nil
      }

      // Must run after the metadata update, so that the new files store all columns.
      val rewriteActions =
        ReplacePartitionedByUtils.rewriteFiles(sparkSession, txn, snapshot, filesToRewrite)

      val stringPrefixLength = conf.getConf(DeltaSQLConf.DATA_SKIPPING_STRING_PREFIX_LENGTH)
      val partitionFields = physicalPartitionSchema.fields.toSeq
      val rewrittenPaths = sparkSession.sparkContext.broadcast(filesToRewrite.map(_.path).toSet)
      import org.apache.spark.sql.delta.implicits._
      val newAddFiles = snapshot.allFiles.mapPartitions { files =>
        val skip = rewrittenPaths.value
        files.filterNot(file => skip.contains(file.path)).map { file =>
          file.copy(
            partitionValues = Map.empty,
            dataChange = false,
            stats = ReplacePartitionedByUtils.addPartitionValuesToStats(
              file.stats, file.partitionValues, partitionFields, stringPrefixLength))
        }
      }

      val actions: Iterator[Action] =
        rewriteActions.iterator ++ newAddFiles.toLocalIterator().asScala ++ domainMetadata.iterator
      val metrics = Map(
        "numRewrittenFiles" -> filesToRewrite.size.toString,
        "numRewrittenBytes" -> filesToRewrite.map(_.size).sum.toString,
        "numAddedFilesFromRewrite" ->
          rewriteActions.count(_.isInstanceOf[AddFile]).toString)
      val newProtocolOpt =
        if (txn.protocol != snapshot.protocol) Some(txn.protocol) else None
      txn.commitLarge(
        sparkSession,
        actions,
        newProtocolOpt,
        DeltaOperations.ReplacePartitionedByWithClusterBy(
          oldMetadata.partitionColumns.mkString(","),
          newLogicalClusteringColumns.mkString(",")),
        context = Map.empty,
        metrics = metrics,
        dataChange = Some(false))
    }
    Seq.empty[Row]
  }
}

object ReplacePartitionedByUtils {

  val NUM_EXAMPLE_FILES = 5

  /**
   * Returns the active data files of the snapshot that do not physically store all the given
   * partition columns, by reading the Parquet footers of the files.
   */
  def findFilesWithoutPartitionColumns(
      spark: SparkSession,
      deltaLog: DeltaLog,
      snapshot: Snapshot,
      physicalPartitionColumns: Seq[String]): Seq[AddFile] = {
    val broadcastConf = spark.sparkContext.broadcast(
      new SerializableConfiguration(deltaLog.newDeltaHadoopConf()))
    val dataRootDir = deltaLog.dataPath.toString
    val requiredColumns = physicalPartitionColumns.map(_.toLowerCase(Locale.ROOT))

    import org.apache.spark.sql.delta.implicits._
    snapshot.allFiles.mapPartitions { files =>
      val conf = broadcastConf.value.value
      files.filter { file =>
        val path = DeltaFileOperations.absolutePath(dataRootDir, file.path)
        val status = path.getFileSystem(conf).getFileStatus(path)
        val footer = ParquetFooterReaderShims.readParquetFooter(
          conf, status, ParquetMetadataConverter.SKIP_ROW_GROUPS)
        val fileColumns = footer.getFileMetaData.getSchema.getFields.asScala
          .map(_.getName.toLowerCase(Locale.ROOT)).toSet
        !requiredColumns.forall(fileColumns.contains)
      }
    }.collect().toSeq
  }

  /**
   * Rewrites the given files with the metadata of the transaction, which must not have partition
   * columns anymore, so that the new files store all columns. Deletion vectors are applied, and
   * row IDs and row commit versions are preserved like in OPTIMIZE. Returns the new files and the
   * removal of the given files, all with `dataChange = false`.
   */
  def rewriteFiles(
      spark: SparkSession,
      txn: OptimisticTransaction,
      snapshot: Snapshot,
      files: Seq[AddFile]): Seq[FileAction] = {
    if (files.isEmpty) return Seq.empty
    require(txn.metadata.partitionColumns.isEmpty,
      "The partitioning must be dropped before rewriting the files")
    var input = txn.deltaLog.createDataFrame(
      snapshot, files, actionTypeOpt = Some("ReplacePartitionedBy"))
    input = RowTracking.preserveRowTrackingColumns(input, snapshot)
    val addFiles = txn.writeFiles(input, None, isOptimize = true, Nil).collect {
      case a: AddFile => a.copy(dataChange = false)
      case other =>
        throw new IllegalStateException(
          s"Unexpected action $other with type ${other.getClass} while rewriting files")
    }
    val timestamp = System.currentTimeMillis()
    addFiles ++ files.map(_.removeWithTimestamp(timestamp, dataChange = false))
  }

  /**
   * Adds min/max/nullCount statistics for the given partition columns to the stats JSON of a
   * file, using the file's partition values. Every row in a file has the same value for a
   * partition column, so these statistics are exact. Statistics that already exist are kept, and
   * no statistics are added if the file has none.
   *
   * Min/max values are only synthesized for types whose JSON stats representation is the same as
   * the partition value representation; other types only get a null count.
   */
  def addPartitionValuesToStats(
      stats: String,
      partitionValues: Map[String, String],
      partitionFields: Seq[StructField],
      stringPrefixLength: Int): String = {
    if (stats == null || stats.isEmpty) return stats
    val root = Try(JsonUtils.mapper.readTree(stats)).toOption match {
      case Some(o: ObjectNode) => o
      case _ => return stats
    }
    val numRecords = root.get(DeltaStatistics.NUM_RECORDS)
    if (numRecords == null || !numRecords.canConvertToLong) return stats

    def objectField(name: String): Option[ObjectNode] = root.get(name) match {
      case null => Some(root.putObject(name))
      case o: ObjectNode => Some(o)
      case _ => None
    }
    val minValues = objectField(DeltaStatistics.MIN)
    val maxValues = objectField(DeltaStatistics.MAX)
    val nullCounts = objectField(DeltaStatistics.NULL_COUNT)

    partitionFields.foreach { field =>
      val name = field.name
      partitionValues.getOrElse(name, null) match {
        case null =>
          nullCounts.filterNot(_.has(name)).foreach(_.put(name, numRecords.asLong))
        case value =>
          nullCounts.filterNot(_.has(name)).foreach(_.put(name, 0L))
          toStatsValue(value, field.dataType, stringPrefixLength).foreach { v =>
            minValues.filterNot(_.has(name)).foreach(_.set[JsonNode](name, v))
            maxValues.filterNot(_.has(name)).foreach(_.set[JsonNode](name, v))
          }
      }
    }
    JsonUtils.mapper.writeValueAsString(root)
  }

  private def toStatsValue(value: String, dataType: DataType, stringPrefixLength: Int)
      : Option[JsonNode] = dataType match {
    // Longer strings would need to be truncated the same way as collected stats.
    case StringType if value.length <= stringPrefixLength => Some(TextNode.valueOf(value))
    case ByteType | ShortType | IntegerType | LongType =>
      Try(value.trim.toLong).toOption.map(LongNode.valueOf)
    case _: DecimalType =>
      Try(new java.math.BigDecimal(value.trim)).toOption.map(DecimalNode.valueOf)
    case DateType =>
      Try(java.time.LocalDate.parse(value.trim)).toOption.map(d => TextNode.valueOf(d.toString))
    case _ => None
  }
}
