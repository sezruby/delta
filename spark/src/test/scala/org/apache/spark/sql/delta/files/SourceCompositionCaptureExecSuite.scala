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

package org.apache.spark.sql.delta.files

import java.io.File

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Action, AddFile, CompactionInfoEntry, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.rdd.{InputFileBlockHolder, RDD}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Tests for the source-composition capture a compaction OPTIMIZE performs.
 *
 * The columnar path of [[SourceCompositionCaptureExec]] is exercised directly with a stub columnar
 * child, since a columnar execution backend is not available in the OSS test harness; both the row
 * and columnar paths fold units into runs through the same `RunTracker`. The end-to-end tests then
 * run a real compaction OPTIMIZE and assert what the write side persists on the removed-source
 * tombstones: contiguous per-source composition tags when the capture is trustworthy, and a safe
 * fall back to plain untagged tombstones (so a losing DML aborts as today) when it is not.
 */
class SourceCompositionCaptureExecSuite extends QueryTest with DeltaSQLCommandTest {

  test("columnar path folds one run per source file across batches, in write order") {
    val acc = new SourceCompositionAccumulator
    spark.sparkContext.register(acc)
    // Two batches from fileA then one from fileB, as a coalesced scan would feed them.
    val child = FakeColumnarScan(Seq(("fileA", 3), ("fileA", 2), ("fileB", 4)))
    // Force the columnar RDD: batches pass through and runs flush to the accumulator on completion.
    SourceCompositionCaptureExec(child, acc).executeColumnar().foreach(_ => ())

    assert(acc.value.size() == 1, "a single partition contributes exactly one entry")
    val runs = acc.value.get(0).asScala.toSeq
    // fileA's two batches fold into one run of 5; fileB starts a new run at the boundary.
    assert(runs == Seq(SourceRun("fileA", 5), SourceRun("fileB", 4)))
  }

  test("interleaved source batches surface as separate runs (the mixed shape the gate rejects)") {
    val acc = new SourceCompositionAccumulator
    spark.sparkContext.register(acc)
    // fileA, fileB, then fileA again -- the interleaving a split-and-packed scan can produce when a
    // single source file is broken into row-group splits that pack non-adjacently.
    val child = FakeColumnarScan(Seq(("fileA", 3), ("fileB", 4), ("fileA", 2)))
    SourceCompositionCaptureExec(child, acc).executeColumnar().foreach(_ => ())

    assert(acc.value.size() == 1)
    val runs = acc.value.get(0).asScala.toSeq
    // fileA is NOT folded across fileB: it appears as two runs. A downstream one-run-per-file gate
    // therefore sees fileA twice and declines to record a (mixed) composition -- reconcile aborts.
    assert(runs == Seq(SourceRun("fileA", 3), SourceRun("fileB", 4), SourceRun("fileA", 2)))
  }

  test("supportsColumnar mirrors the child so the operator stays columnar-transparent") {
    val acc = new SourceCompositionAccumulator
    assert(SourceCompositionCaptureExec(FakeColumnarScan(Nil), acc).supportsColumnar)
    assert(!SourceCompositionCaptureExec(FakeRowScan(), acc).supportsColumnar)
  }

  test("compaction OPTIMIZE tags each multi-row-group source as one contiguous run") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val prevBlockSize = hadoopConf.get("parquet.block.size")
      // A tiny row-group size so each source file is written as MANY row groups -- the multi-piece
      // read that would defeat capture if a source were split across partitions. The pinned read
      // must still land each source whole, folding its row groups into one contiguous run.
      hadoopConf.set("parquet.block.size", "1024")
      try {
        // Two differently sized sources so contiguity is observable regardless of write order.
        spark.range(0, 3000).repartition(1).write.format("delta").mode("append").save(path)
        spark.range(3000, 5000).repartition(1).write.format("delta").mode("append").save(path)
      } finally {
        if (prevBlockSize == null) hadoopConf.unset("parquet.block.size")
        else hadoopConf.set("parquet.block.size", prevBlockSize)
      }

      // Precondition: each source really is multi-row-group (otherwise the test proves nothing).
      val rowGroups = rowGroupCountsPerFile(dir)
      assert(rowGroups.size == 2 && rowGroups.forall(_ > 1),
        s"expected two multi-row-group sources, got $rowGroups")

      // A hostile ambient split size: 8 KiB is smaller than either source (~11.6 KiB / ~17.6 KiB),
      // so each is broken into a full split plus a row-bearing remainder. Under coalesce(1)'s
      // descending-length split packing that remainder is read after the other source's head, so
      // WITHOUT the pins the two sources interleave -- each would surface to the capture as more
      // than one run, the one-run-per-file gate would decline, and the tags asserted below would be
      // absent. The capture path pins the read against exactly this: it clones the session and sets
      // maxPartitionBytes to the compaction target (>= every source) and minPartitionNum = 1, so
      // each source reads whole in one partition and its row groups fold into one contiguous run.
      // Drop the pins in readCompactionSourceWithWholeFilePins and this test fails.
      withSQLConf(
          DeltaSQLConf.DELTA_OPTIMIZE_CONFLICT_RECONCILIATION_ENABLED.key -> "true",
          SQLConf.FILES_MAX_PARTITION_BYTES.key -> "8192") {
        sql(s"OPTIMIZE delta.`$path`")
      }

      val actions = optimizeCommitActions(path)
      val adds = actions.collect { case a: AddFile => a }
      val removes = actions.collect { case r: RemoveFile => r }
      assert(adds.size == 1, "the bin compacts into a single output file")
      assert(removes.size == 2, "both sources are removed")

      // Every source is tagged and points at the one output.
      val output = adds.head.path
      assert(removes.forall { r =>
        r.getTag(RemoveFile.Tags.COMPACTED_INTO)
          .map(JsonUtils.fromJson[Seq[String]]).contains(Seq(output))
      }, "each source must record the output it compacted into")

      // (offset, physicalCount) for each source, in output order. No source DVs here, so
      // physical == live.
      val runs = removes
        .map(r => compactionInfo(r).get.head)
        .map(e => (e.rowOffsetInTarget.get, e.sourceNumPhysicalRecords.get))
        .sortBy(_._1)
      // The runs tile the output contiguously from offset 0, with no gaps or overlaps.
      assert(runs.head._1 == 0L, s"first run must start at offset 0: $runs")
      assert(runs(1)._1 == runs(0)._1 + runs(0)._2, s"runs are not contiguous: $runs")
      assert(runs.map(_._2).sum == 5000L, s"runs must cover every output row: $runs")
      assert(runs.map(_._2).toSet == Set(2000L, 3000L), s"unexpected run sizes: $runs")
    }
  }

  test("a source without row-count stats fails the gate -> untagged tombstones (aborts as today)") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      // Write the sources with stats collection OFF, so their AddFiles carry no numLogicalRecords
      // -- one of the trustworthiness conditions the capture gate requires.
      withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
        spark.range(0, 2000).repartition(1).write.format("delta").mode("append").save(path)
        spark.range(2000, 4000).repartition(1).write.format("delta").mode("append").save(path)
      }
      // Sanity: the sources indeed lack the stat the gate checks.
      val deltaLog = DeltaLog.forTable(spark, path)
      assert(deltaLog.update().allFiles.collect().forall(_.numLogicalRecords.isEmpty),
        "sources must have no row-count stats for this test to exercise the gate")

      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_CONFLICT_RECONCILIATION_ENABLED.key -> "true") {
        sql(s"OPTIMIZE delta.`$path`")
      }

      val actions = optimizeCommitActions(path)
      val adds = actions.collect { case a: AddFile => a }
      val removes = actions.collect { case r: RemoveFile => r }
      assert(adds.size == 1 && removes.size == 2, "the sources are still compacted")
      // Capture ran (reconcile was on) but the missing stats make it untrustworthy: the write side
      // must fall back to plain untagged tombstones, so a losing DML aborts exactly as it does
      // today -- never a bogus offset remap.
      assert(removes.forall(_.getTag(RemoveFile.Tags.COMPACTED_INTO).isEmpty))
      assert(removes.forall(_.getTag(RemoveFile.Tags.COMPACTION_INFO).isEmpty))
      // The compaction itself is otherwise a normal OPTIMIZE: data is intact.
      checkAnswer(spark.read.format("delta").load(path), (0 until 4000).map(i => Row(i.toLong)))
    }
  }

  test("repartition OPTIMIZE writes no composition tags (capture is coalesce-only)") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 2000).repartition(1).write.format("delta").mode("append").save(path)
      spark.range(2000, 4000).repartition(1).write.format("delta").mode("append").save(path)

      // Reconcile is on, but the repartition compaction path shuffles rows into the output, so no
      // source keeps a contiguous row range -- an offset composition would be meaningless (and a
      // DV remapped by it would corrupt data). The capture gate excludes this path, so the sources
      // must be removed with plain untagged tombstones, exactly as vanilla OPTIMIZE writes.
      withSQLConf(
          DeltaSQLConf.DELTA_OPTIMIZE_CONFLICT_RECONCILIATION_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_OPTIMIZE_REPARTITION_ENABLED.key -> "true") {
        sql(s"OPTIMIZE delta.`$path`")
      }

      val actions = optimizeCommitActions(path)
      val adds = actions.collect { case a: AddFile => a }
      val removes = actions.collect { case r: RemoveFile => r }
      assert(adds.size == 1 && removes.size == 2, "the sources are still compacted")
      assert(removes.forall(_.getTag(RemoveFile.Tags.COMPACTED_INTO).isEmpty),
        "the repartition path must not record where sources landed -- rows were shuffled")
      assert(removes.forall(_.getTag(RemoveFile.Tags.COMPACTION_INFO).isEmpty),
        "the repartition path must not record where sources landed -- rows were shuffled")
      checkAnswer(spark.read.format("delta").load(path), (0 until 4000).map(i => Row(i.toLong)))
    }
  }

  test("ZORDER OPTIMIZE writes no composition tags (rows are z-ordered, not contiguous)") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 2000).repartition(1).write.format("delta").mode("append").save(path)
      spark.range(2000, 4000).repartition(1).write.format("delta").mode("append").save(path)

      // A ZORDER pass reorders rows onto a space-filling curve, so no source keeps a contiguous
      // row range and an offset composition would be meaningless. The capture gate excludes the
      // multi-dimensional-clustering path, so the sources must be removed with plain untagged
      // tombstones.
      withSQLConf(
          DeltaSQLConf.DELTA_OPTIMIZE_CONFLICT_RECONCILIATION_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_OPTIMIZE_ZORDER_COL_STAT_CHECK.key -> "false") {
        sql(s"OPTIMIZE delta.`$path` ZORDER BY (id)")
      }

      val actions = optimizeCommitActions(path)
      val removes = actions.collect { case r: RemoveFile => r }
      assert(actions.exists(_.isInstanceOf[AddFile]) && removes.nonEmpty,
        "the z-order must rewrite files")
      assert(removes.forall(_.getTag(RemoveFile.Tags.COMPACTED_INTO).isEmpty),
        "z-order must not record a row-range composition -- rows were permuted")
      assert(removes.forall(_.getTag(RemoveFile.Tags.COMPACTION_INFO).isEmpty),
        "z-order must not record a row-range composition -- rows were permuted")
      checkAnswer(spark.read.format("delta").load(path), (0 until 4000).map(i => Row(i.toLong)))
    }
  }

  test("CLUSTER BY OPTIMIZE writes no composition tags (clustering permutes rows)") {
    withTable("clustered_optimize_src") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        sql(s"CREATE TABLE clustered_optimize_src (id LONG) USING delta " +
          s"CLUSTER BY (id) LOCATION '$path'")
        sql("INSERT INTO clustered_optimize_src SELECT id FROM range(0, 2000)")
        sql("INSERT INTO clustered_optimize_src SELECT id FROM range(2000, 4000)")

        // A clustering pass reorders rows into ZCubes, so no source keeps a contiguous row range.
        // Same gate as ZORDER (isMultiDimClustering): the sources must be removed with plain
        // untagged tombstones.
        withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_CONFLICT_RECONCILIATION_ENABLED.key -> "true") {
          sql("OPTIMIZE clustered_optimize_src")
        }

        val actions = optimizeCommitActions(path)
        val removes = actions.collect { case r: RemoveFile => r }
        assert(actions.exists(_.isInstanceOf[AddFile]) && removes.nonEmpty,
          "the clustering pass must rewrite files")
        assert(removes.forall(_.getTag(RemoveFile.Tags.COMPACTED_INTO).isEmpty),
          "clustering must not record a row-range composition -- rows were permuted")
        assert(removes.forall(_.getTag(RemoveFile.Tags.COMPACTION_INFO).isEmpty),
          "clustering must not record a row-range composition -- rows were permuted")
        checkAnswer(spark.table("clustered_optimize_src"), (0 until 4000).map(i => Row(i.toLong)))
      }
    }
  }

  /** Actions committed by the single OPTIMIZE at the table's current (latest) version. */
  private def optimizeCommitActions(path: String): Seq[Action] = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val optimizeVersion = deltaLog.update().version
    deltaLog.getChanges(startVersion = optimizeVersion, catalogTableOpt = None).next()._2
  }

  /** The compaction composition recorded on a source tombstone, if it was tagged. */
  private def compactionInfo(r: RemoveFile): Option[Seq[CompactionInfoEntry]] =
    r.getTag(RemoveFile.Tags.COMPACTION_INFO).map(JsonUtils.fromJson[Seq[CompactionInfoEntry]])

  /** Number of Parquet row groups in each data file physically present under the table dir. */
  private def rowGroupCountsPerFile(dir: File): Seq[Int] = {
    // scalastyle:off deltahadoopconfiguration
    val conf = spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
    dir.listFiles().filter(_.getName.endsWith(".parquet")).toSeq.map { f =>
      val input = HadoopInputFile.fromPath(new Path(f.getAbsolutePath), conf)
      val reader = ParquetFileReader.open(input)
      try reader.getRowGroups.size() finally reader.close()
    }
  }
}

/**
 * A leaf that emits one columnar batch per `(file, rowCount)` spec, setting the scan's thread-local
 * file identity before each batch just as a real file scan does. One partition, in spec order.
 */
private case class FakeColumnarScan(specs: Seq[(String, Int)]) extends LeafExecNode {
  override def output: Seq[Attribute] = Seq(AttributeReference("v", IntegerType)())

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("columnar only")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val specsLocal = specs
    sparkContext.parallelize(Seq(specsLocal), numSlices = 1).flatMap { batchSpecs =>
      batchSpecs.iterator.map { case (file, n) =>
        InputFileBlockHolder.set(file, 0L, (n * 4).toLong)
        val vec = new OnHeapColumnVector(math.max(n, 1), IntegerType)
        var i = 0
        while (i < n) {
          vec.putInt(i, 1)
          i += 1
        }
        new ColumnarBatch(Array[ColumnVector](vec), n)
      }
    }
  }
}

/** A leaf that supports only the row path (`supportsColumnar` defaults to false). */
private case class FakeRowScan() extends LeafExecNode {
  override def output: Seq[Attribute] = Seq(AttributeReference("v", IntegerType)())

  override protected def doExecute(): RDD[InternalRow] = sparkContext.emptyRDD[InternalRow]
}
