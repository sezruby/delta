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

import scala.collection.mutable

import org.apache.spark.rdd.{InputFileBlockHolder, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{AccumulatorV2, CompletionIterator}

/** A contiguous run of `count` live rows from one source file, landing at consecutive positions in
 *  a compaction output in write order. `count` is the rows the write actually saw (the scan has
 *  already applied any compaction-time deletion vector), so a DV'd source is still one run of its
 *  live rows; the physical row_index of each live row is reconstructed at conflict-resolution time
 *  from the source's read-time deletion vector, not recorded here. */
case class SourceRun(sourceFile: String, count: Long)

/**
 * Accumulates, per write task, the ordered [[SourceRun]]s observed while a compaction OPTIMIZE
 * writes its output. Each successful task contributes one entry (the runs it saw, in write order).
 * The driver expects exactly one entry (a single-output compaction bin, one partition); anything
 * else (speculation, a split into multiple files) is treated as unreconcilable and the tag is
 * dropped, so the loser simply aborts as it does today.
 */
class SourceCompositionAccumulator
  extends AccumulatorV2[Seq[SourceRun], java.util.List[java.util.List[SourceRun]]] {

  private val partitionRuns = new java.util.ArrayList[java.util.List[SourceRun]]()

  override def isZero: Boolean = partitionRuns.isEmpty

  override def copy(): SourceCompositionAccumulator = {
    val c = new SourceCompositionAccumulator
    c.partitionRuns.addAll(partitionRuns)
    c
  }

  override def reset(): Unit = partitionRuns.clear()

  override def add(runs: Seq[SourceRun]): Unit = {
    val list = new java.util.ArrayList[SourceRun](runs.length)
    runs.foreach(list.add)
    partitionRuns.add(list)
  }

  override def merge(
      other: AccumulatorV2[Seq[SourceRun], java.util.List[java.util.List[SourceRun]]]): Unit =
    partitionRuns.addAll(other.value)

  override def value: java.util.List[java.util.List[SourceRun]] = partitionRuns
}

/**
 * Per write-task state shared by the row and columnar execution paths: reads the scan's
 * thread-local file identity ([[InputFileBlockHolder]]) and folds consecutive same-file units --
 * a single row, or a whole columnar batch -- into one [[SourceRun]], in observed write order.
 */
private class RunTracker {
  private val runs = mutable.ArrayBuffer.empty[SourceRun]
  // Holder instance for the current file (stable per file, so `eq` fast-paths the same-file hot
  // path); the path string is materialized once per boundary, not per unit.
  private var curFileUtf: UTF8String = null
  private var curFile: String = null
  private var curCount = 0L

  private def closeRun(): Unit = if (curCount > 0) runs += SourceRun(curFile, curCount)

  /**
   * Fold `delta` more rows of the current scan file into the open run, starting a new run when the
   * thread-local file identity changes. `delta` is 1 for a row, or the batch row count for a batch.
   */
  def observe(delta: Long): Unit = {
    val sfUtf = InputFileBlockHolder.getInputFilePath
    val sameFile = (sfUtf eq curFileUtf) || (sfUtf != null && sfUtf.equals(curFileUtf))
    if (sameFile) {
      curCount += delta
    } else {
      closeRun()
      curFileUtf = sfUtf
      curFile = if (sfUtf == null || sfUtf.numBytes() == 0) null else sfUtf.toString
      curCount = delta
    }
  }

  /** Close the final open run and return the ordered runs this task observed. */
  def finish(): Seq[SourceRun] = {
    closeRun()
    runs.toSeq
  }
}

/**
 * A write-stage operator for OPTIMIZE compaction conflict-reconciliation, injected into the write
 * plan (like [[DeltaOptimizedWriterExec]]). It records, per source file, how many rows that file
 * contributed to the compaction output and in what order, with no per-row helper column.
 *
 * The source file identity is read per row from [[InputFileBlockHolder]] (the scan's thread-local,
 * the same one `input_file_name()` reads); a per-file counter tracks each file's live row count.
 * Rows are emitted unchanged: there is no extra column to strip (no row copy) and no
 * `_metadata.row_index` materialization (which would drag in the DV-aware scan cost). On a
 * contiguous coalesce read each file's live rows land in one output segment, so `(sourceFile,
 * count)` in write order fully describes the layout and the driver derives the output offsets. A
 * source that had a compaction-time deletion vector is still one run (of its live rows); its
 * physical row_index gaps are reconstructed at conflict time, so no DV is read here.
 *
 * The operator handles both execution modes. In row mode the identity is read once per row; when
 * the child produces columnar batches (a vectorized / native execution backend), it stays columnar
 * -- reading the identity once per batch and passing the batch through unchanged -- so it adds no
 * columnar-to-row transition (which would materialize every batch just to observe it). A batch
 * holds rows from a single scan file, so one read per batch is exact. Either mode folds units into
 * runs through the same [[RunTracker]], so the two paths are identical by construction.
 *
 * Runs flush to the accumulator on successful task completion; failed attempts flush nothing. The
 * [[InputFileBlockHolder]] read is only valid when the scan and this operator run in the same task
 * with no shuffle between them (the coalesce path, the default). On the repartition path the holder
 * is empty after the shuffle, so no file is recorded and the loser aborts as it does today.
 */
case class SourceCompositionCaptureExec(
    child: SparkPlan,
    acc: SourceCompositionAccumulator,
    childOutputOrdering: Seq[SortOrder] = Nil) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  // For a partitioned OPTIMIZE, DeltaFileFormatWriter's requiredOrdering is the partition column,
  // which is constant within a single compaction bin, so the output IS trivially ordered by it.
  // Reporting that ordering keeps `orderingMatched` true and stops the writer from inserting a
  // SortExec ABOVE this operator, which would reorder rows and invalidate the captured write-order
  // offsets. Falls back to the child's ordering when unset (the unpartitioned case, where the
  // writer's requiredOrdering is already empty).
  override def outputOrdering: Seq[SortOrder] =
    if (childOutputOrdering.nonEmpty) childOutputOrdering else child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    val accumulator = acc
    child.execute().mapPartitions { iter =>
      val tracker = new RunTracker
      // One row observed per element; rows pass through unchanged (no helper column, no copy).
      val mapped = iter.map { row => tracker.observe(1L); row }
      // Flush the observed runs once the writer has consumed the whole partition. Using
      // CompletionIterator (rather than a task-completion listener) runs the flush inside the task
      // body, so the accumulator update is collected and propagated to the driver.
      CompletionIterator[InternalRow, Iterator[InternalRow]](
        mapped, accumulator.add(tracker.finish()))
    }
  }

  // Stay columnar when the child is (a vectorized / native execution backend): a row-only operator
  // would force a columnar-to-row transition here just to observe the file identity.
  override def supportsColumnar: Boolean = child.supportsColumnar

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val accumulator = acc
    child.executeColumnar().mapPartitions { iter =>
      val tracker = new RunTracker
      val mapped = iter.map { batch =>
        // A columnar batch holds rows from a single scan file, so the file identity is read once
        // per batch (not per row) and the whole batch's row count extends the current run.
        val n = batch.numRows()
        if (n > 0) tracker.observe(n.toLong)
        batch // pass through unchanged
      }
      CompletionIterator[ColumnarBatch, Iterator[ColumnarBatch]](
        mapped, accumulator.add(tracker.finish()))
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SourceCompositionCaptureExec =
    copy(child = newChild)
}
