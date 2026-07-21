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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{AccumulatorV2, CompletionIterator}

/** A contiguous run of rows from one source file, landing at consecutive positions in a
 *  compaction output. Rows `[startRowIndex, startRowIndex + count)` of `sourceFile` occupy the
 *  next `count` output positions in write order. For a source file with no compaction-time
 *  deletion vector this is one run with `startRowIndex = 0`; DV'd files are left unreconciled. */
case class SourceRun(sourceFile: String, startRowIndex: Long, count: Long)

object SourceCompositionCaptureExec {
  /** No transient helper column is needed: the source file identity comes from
   *  [[InputFileBlockHolder]] and the per-file row count is derived by counting output rows, so
   *  nothing extra is materialized into the row and the writer sees a plain table-width row. */
  val captureColumns: Set[String] = Set.empty
}

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
 * A write-stage operator for OPTIMIZE compaction conflict-reconciliation, injected into the write
 * plan (like [[DeltaOptimizedWriterExec]]). It records, per source file, how many rows that file
 * contributed to the compaction output and in what order, with no per-row helper column.
 *
 * The source file identity is read per row from [[InputFileBlockHolder]] (the scan's thread-local,
 * the same one `input_file_name()` reads); a per-file counter tracks each file's row count. Rows
 * are emitted unchanged: there is no extra column to strip (no row copy) and no
 * `_metadata.row_index` materialization (which would drag in the DV-aware scan cost). On a
 * contiguous coalesce read each file's rows land in one output segment, so `(sourceFile, count)` in
 * write order fully describes the layout and the driver derives the physical offsets. Each run uses
 * `startRowIndex = 0`, valid only when the source file had no compaction-time deletion vector; the
 * driver tags only in that case and otherwise aborts.
 *
 * Runs flush to the accumulator on successful task completion; failed attempts flush nothing. The
 * [[InputFileBlockHolder]] read is only valid when the scan and this operator run in the same task
 * with no shuffle between them (the coalesce path, the default). On the repartition path the holder
 * is empty after the shuffle, so no file is recorded and the loser aborts as it does today.
 */
case class SourceCompositionCaptureExec(
    child: SparkPlan,
    acc: SourceCompositionAccumulator) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def doExecute(): RDD[InternalRow] = {
    val accumulator = acc
    child.execute().mapPartitions { iter =>
      val runs = mutable.ArrayBuffer.empty[SourceRun]
      var curFileUtf: UTF8String = null // holder instance for the current file (stable per file)
      var curFile: String = null // materialized once per file boundary, not per row
      var curCount = 0L
      def closeRun(): Unit = if (curCount > 0) runs += SourceRun(curFile, 0L, curCount)

      val mapped = iter.map { row =>
        // File identity from the scan's thread-local; the holder returns a stable UTF8String per
        // file, so `eq` fast-paths the same-file hot path and we only call toString at a boundary.
        val sfUtf = InputFileBlockHolder.getInputFilePath
        val sameFile = (sfUtf eq curFileUtf) || (sfUtf != null && sfUtf.equals(curFileUtf))
        if (sameFile) {
          curCount += 1
        } else {
          closeRun()
          curFileUtf = sfUtf
          curFile = if (sfUtf == null || sfUtf.numBytes() == 0) null else sfUtf.toString
          curCount = 1
        }
        row // pass through unchanged; no helper column exists to strip
      }
      // Flush the observed runs once the writer has consumed the whole partition. Using
      // CompletionIterator (rather than a task-completion listener) runs the flush inside the task
      // body, so the accumulator update is collected and propagated to the driver.
      CompletionIterator[InternalRow, Iterator[InternalRow]](mapped, {
        closeRun()
        accumulator.add(runs.toSeq)
      })
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SourceCompositionCaptureExec =
    copy(child = newChild)
}
