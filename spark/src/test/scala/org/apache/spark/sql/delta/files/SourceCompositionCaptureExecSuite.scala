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

import scala.jdk.CollectionConverters._

import org.apache.spark.rdd.{InputFileBlockHolder, RDD}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Unit tests for [[SourceCompositionCaptureExec]]'s columnar execution path. The row path is
 * covered end to end by `OptimizeConflictReconciliationSuite`; here the columnar path is exercised
 * directly with a stub columnar child, since a columnar execution backend is not available in the
 * OSS test harness. Both paths fold units into runs through the same `RunTracker`.
 */
class SourceCompositionCaptureExecSuite extends QueryTest with SharedSparkSession {

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

  test("supportsColumnar mirrors the child so the operator stays columnar-transparent") {
    val acc = new SourceCompositionAccumulator
    assert(SourceCompositionCaptureExec(FakeColumnarScan(Nil), acc).supportsColumnar)
    assert(!SourceCompositionCaptureExec(FakeRowScan(), acc).supportsColumnar)
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
