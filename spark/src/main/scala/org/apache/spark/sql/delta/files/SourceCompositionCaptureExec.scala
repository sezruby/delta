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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.util.{AccumulatorV2, CompletionIterator}

/** A contiguous run of rows from one source file, landing at consecutive positions in a
 *  compaction output. Rows `[startRowIndex, startRowIndex + count)` of `sourceFile` occupy the
 *  next `count` output positions in write order. */
case class SourceRun(sourceFile: String, startRowIndex: Long, count: Long)

object SourceCompositionCaptureExec {
  /** Transient helper column carrying the source file path (`_metadata.file_path`). */
  final val SOURCE_FILE_COL = "__source_file"
  /** Transient helper column carrying the physical source row index (`_metadata.row_index`). */
  final val SOURCE_ROW_INDEX_COL = "__source_row_index"
  /** The transient helper columns observed by the capture and never persisted. */
  val captureColumns: Set[String] = Set(SOURCE_FILE_COL, SOURCE_ROW_INDEX_COL)
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
 * plan (like [[DeltaOptimizedWriterExec]]). Its `child` produces the table columns plus two
 * transient helper columns `__source_file` (`_metadata.file_path`, at `sourceFileOrdinal`) and
 * `__source_row_index` (`_metadata.row_index`, at `sourceRowIndexOrdinal`). As rows stream through
 * in the exact order they are written, it records the contiguous `(sourceFile, startRowIndex,
 * count)` runs via [[acc]] and emits each row **projected to just [[keptOutput]] (the table
 * columns)** so the helpers are observed but never persisted, and `output` is a clean table
 * schema for the writer/stats.
 *
 * Runs flush to the accumulator on successful task completion (the writer consumed the whole
 * partition and committed); failed attempts flush nothing. Correct regardless of read order / file
 * splits, because it records what actually happened rather than assuming an order.
 */
case class SourceCompositionCaptureExec(
    child: SparkPlan,
    keptOutput: Seq[Attribute],
    sourceFileOrdinal: Int,
    sourceRowIndexOrdinal: Int,
    acc: SourceCompositionAccumulator) extends UnaryExecNode {

  override def output: Seq[Attribute] = keptOutput

  override def doExecute(): RDD[InternalRow] = {
    val childOutput = child.output
    val kept = keptOutput
    val sfOrd = sourceFileOrdinal
    val riOrd = sourceRowIndexOrdinal
    val accumulator = acc
    child.execute().mapPartitions { iter =>
      val project = UnsafeProjection.create(kept, childOutput)
      val runs = mutable.ArrayBuffer.empty[SourceRun]
      var curFile: String = null
      var curStart = 0L
      var curCount = 0L
      var curNext = 0L // expected next source row index to continue the current run
      def closeRun(): Unit = if (curCount > 0) runs += SourceRun(curFile, curStart, curCount)

      val mapped = iter.map { row =>
        val sf = if (row.isNullAt(sfOrd)) null else row.getUTF8String(sfOrd).toString
        val ri = if (row.isNullAt(riOrd)) -1L else row.getLong(riOrd)
        if (sf != null && sf == curFile && ri == curNext) {
          curCount += 1
          curNext += 1
        } else {
          closeRun()
          curFile = sf
          curStart = ri
          curCount = 1
          curNext = ri + 1
        }
        project(row)
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
