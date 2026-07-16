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

import java.io.File

import scala.concurrent.duration.Duration

import org.apache.spark.sql.delta.concurrency.{PhaseLockingTestMixin, TransactionExecutionTestMixin}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

/**
 * Tests for compaction OPTIMIZE vs concurrent row-level DML reconciliation
 * (spark.databricks.delta.optimize.conflictReconciliation.enabled): a compaction OPTIMIZE that
 * loses to a concurrent DELETE remaps the concurrent deletion vector onto the compacted output
 * (offset arithmetic) instead of aborting. Reclustering / already-DV'd sources are left to abort.
 */
class OptimizeConflictReconciliationSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with PhaseLockingTestMixin
  with TransactionExecutionTestMixin {

  // Deletion vectors on for every table in this suite.
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey, "true")

  private def tableRef(dir: File): String = s"delta.`${dir.getCanonicalPath}`"

  /** Multi-file table: id in [0, n) across `files` data files, deletion vectors enabled. */
  private def createMultiFileTable(dir: File, n: Int = 300, files: Int = 3): DeltaLog = {
    spark.range(start = 0, end = n, step = 1, numPartitions = files)
      .write.format("delta").mode("append").save(dir.getAbsolutePath)
    val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
    assert(log.update().allFiles.collect().length === files,
      s"test table must have $files data files")
    log
  }

  /** Runs `sqlText` with the OPTIMIZE conflict-reconciliation flag set to `reconcile`. */
  private def sqlTxn(sqlText: String, reconcile: Boolean): () => Array[Row] =
    () => {
      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_CONFLICT_RECONCILIATION_ENABLED.key -> reconcile.toString) {
        sql(sqlText).collect()
      }
      Array.empty[Row]
    }

  private def ids(dir: File): Seq[Long] =
    spark.read.format("delta").load(dir.getAbsolutePath).select("id")
      .collect().map(_.getLong(0)).sorted.toSeq

  private def deletionVectorCardinalities(log: DeltaLog): Seq[Long] =
    log.update().allFiles.collect()
      .filter(_.deletionVector != null)
      .map(_.deletionVector.cardinality)
      .toSeq

  test("compaction OPTIMIZE (loser) reconciles a concurrent DELETE by remapping the DV") {
    withTempDir { dir =>
      val log = createMultiFileTable(dir)
      // A (loser): OPTIMIZE compacts all files. B (winner): DELETE id=150 commits during A, adding
      // a DV to the middle source file. A must remap B's DV onto the compacted output at commit.
      val txnA = sqlTxn(s"OPTIMIZE ${tableRef(dir)}", reconcile = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 150", reconcile = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureA, Duration.Inf)

      // Both committed, correct data: id=150 gone, everything else present.
      assert(ids(dir) === (0L until 300L).filterNot(_ == 150L))
      // Compacted to a single output file that carries the remapped deletion vector (cardinality 1),
      // proving reconciliation (a plain re-compaction retry would leave no DV on the output).
      val files = log.update().allFiles.collect()
      assert(files.length === 1, s"expected a single compacted file, got ${files.length}")
      assert(deletionVectorCardinalities(log) === Seq(1L),
        "compacted output should carry the remapped deletion vector")
    }
  }
}
