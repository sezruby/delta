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

import org.apache.spark.sql.delta.actions.{DeletionVectorDescriptor, RemoveFile}
import org.apache.spark.sql.delta.concurrency.{PhaseLockingTestMixin, TransactionExecutionTestMixin}
import org.apache.spark.sql.delta.files.{DeltaFileFormatWriter, SourceCompositionCaptureExec}
import org.apache.spark.sql.delta.fuzzer.{OptimisticTransactionPhases, PhaseLockingTransactionExecutionObserver}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

/**
 * Tests for compaction OPTIMIZE vs concurrent row-level DML reconciliation
 * (spark.databricks.delta.optimize.conflictReconciliation.enabled): a compaction OPTIMIZE that
 * loses to a concurrent DELETE/UPDATE remaps the concurrent deletion vector onto the compacted
 * output (offset arithmetic, with read-time DV gaps reconstructed lazily) instead of aborting.
 * Also covers the safety fallbacks that must NOT reconcile -- reclustering (ZORDER), the
 * repartition path, and the reverse (DELETE-loser) direction -- which abort as they do today.
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

  /**
   * Runs `sqlText` with the OPTIMIZE conflict-reconciliation flag set to `reconcile`, plus any
   * `extraConf` overrides (e.g. forcing the repartition path or a small max file size).
   */
  private def sqlTxn(
      sqlText: String,
      reconcile: Boolean,
      extraConf: Seq[(String, String)] = Nil): () => Array[Row] =
    () => {
      val confs = (DeltaSQLConf.DELTA_OPTIMIZE_CONFLICT_RECONCILIATION_ENABLED.key ->
        reconcile.toString) +: extraConf
      withSQLConf(confs: _*) {
        sql(sqlText).collect()
      }
      Array.empty[Row]
    }

  /**
   * Runs `optimizeSql` as the loser while a concurrent `DELETE id = deleteId` wins (commits during
   * the OPTIMIZE's first attempt), and returns the exception the OPTIMIZE ultimately fails with.
   *
   * OPTIMIZE auto-resolves and RETRIES on abort, so the single-observer `runTxnsWithOrder` helpers
   * cannot drive it -- the retry's second commit is an unexpected phase transition. This mirrors
   * upstream `OptimizeConflictSuite`: give the retry its own [[PhaseLockingTransactionExecutionObserver]]
   * chained via `setNextObserver`, then let the abort surface.
   */
  private def runOptimizeLoserExpectingAbort(
      dir: File,
      optimizeSql: String,
      deleteId: Long,
      extraConf: Seq[(String, String)] = Nil): SparkException = {
    val optimizeFn = sqlTxn(optimizeSql, reconcile = true, extraConf)
    val Seq(future) = runFunctionsWithOrderingFromObserver(Seq(optimizeFn)) {
      case (optimizeObserver :: Nil) =>
        val retryObserver = new PhaseLockingTransactionExecutionObserver(
          OptimisticTransactionPhases.forName("test-replacement-txn"))
        optimizeObserver.setNextObserver(retryObserver, autoAdvance = true)
        unblockUntilPreCommit(optimizeObserver)
        busyWaitFor(optimizeObserver.phases.preparePhase.hasEntered, timeout)
        // Winner commits during OPTIMIZE's first attempt, then OPTIMIZE aborts and retries.
        sql(s"DELETE FROM ${tableRef(dir)} WHERE id = $deleteId").collect()
        unblockCommit(optimizeObserver)
        busyWaitFor(optimizeObserver.phases.commitPhase.hasLeft, timeout)
        optimizeObserver.phases.postCommitPhase.exitBarrier.unblock()
        unblockAllPhases(retryObserver)
    }
    intercept[SparkException] { ThreadUtils.awaitResult(future, timeout) }
  }

  /** Asserts an awaited transaction future failed with a Delta concurrency conflict (an abort). */
  private def assertConcurrentModificationException(e: SparkException): Unit = {
    val causeName = e.getCause.getClass.getName
    assert(
      Seq("ConcurrentAppend", "ConcurrentDeleteRead", "ConcurrentDeleteDelete")
        .exists(causeName.contains),
      s"Expected a concurrency conflict, got: $causeName")
  }

  private def ids(dir: File): Seq[Long] =
    spark.read.format("delta").load(dir.getAbsolutePath).select("id")
      .collect().map(_.getLong(0)).sorted.toSeq

  private def deletionVectorCardinalities(log: DeltaLog): Seq[Long] =
    log.update().allFiles.collect()
      .filter(_.deletionVector != null)
      .map(_.deletionVector.cardinality)
      .toSeq

  /**
   * Partitioned (single partition value, `files` data files) variant of [[createMultiFileTable]].
   * A single distinct partition value keeps OPTIMIZE to one compaction bin, so the write is
   * partitioned but still a single output -- the case F1 (sort-above-capture) applies to.
   */
  private def createPartitionedMultiFileTable(dir: File, n: Int = 300, files: Int = 3): DeltaLog = {
    spark.range(start = 0, end = n, step = 1, numPartitions = files)
      .withColumn("part", lit(0))
      .write.format("delta").partitionBy("part").mode("append").save(dir.getAbsolutePath)
    val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
    assert(log.update().allFiles.collect().length === files,
      s"test table must have $files data files")
    log
  }

  /** RemoveFile actions in the table's latest commit (the reconciled OPTIMIZE commit here). */
  private def lastCommitRemoveFiles(log: DeltaLog): Seq[RemoveFile] = {
    val version = log.update().version
    log.getChanges(version).flatMap(_._2).collect { case r: RemoveFile => r }.toSeq
  }

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
      // Compacted to one output file carrying the remapped deletion vector (cardinality 1), proving
      // reconciliation (a plain re-compaction retry would leave no DV on the output).
      val files = log.update().allFiles.collect()
      assert(files.length === 1, s"expected a single compacted file, got ${files.length}")
      assert(deletionVectorCardinalities(log) === Seq(1L),
        "compacted output should carry the remapped deletion vector")
      // F2: every RemoveFile in the reconciled OPTIMIZE commit must be dataChange=false. An OPTIMIZE
      // is a data-preserving relocation; a dataChange=true reconciled tombstone would break
      // streaming/CDC transparency and mix dataChange values within the one commit.
      val removes = lastCommitRemoveFiles(log)
      assert(removes.nonEmpty, "reconciled OPTIMIZE commit should contain source tombstones")
      assert(removes.forall(!_.dataChange),
        "reconciled OPTIMIZE tombstones must all be dataChange=false")
    }
  }

  test("compaction OPTIMIZE (loser) reconciles a DELETE when the source already has a DV") {
    withTempDir { dir =>
      val log = createMultiFileTable(dir)
      // Pre-existing (compaction-time) DV: delete id=100 (physical row 0 of the middle file). This
      // commits before OPTIMIZE reads, so that file's live rows start at physical index 1 and the
      // read-time gap must be reconstructed at conflict time.
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 100")
      assert(deletionVectorCardinalities(log) === Seq(1L), "pre-existing DV expected on one file")

      // A (loser): OPTIMIZE compacts all files (purging the pre-existing DV). B (winner): DELETE
      // id=150 commits during A, giving the middle file a cumulative DV {id100, id150}. A remaps
      // only B's NEW deletion (id150) onto the output, discounting the read-time gap from id100.
      val txnA = sqlTxn(s"OPTIMIZE ${tableRef(dir)}", reconcile = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 150", reconcile = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureA, Duration.Inf)

      // Both committed, correct data: id=100 and id=150 gone, everything else present.
      assert(ids(dir) === (0L until 300L).filterNot(x => x == 100L || x == 150L))
      // Compacted to one output file whose DV carries only the new id=150 (cardinality 1); id=100
      // was already excluded from the output, so it is not in the remapped DV.
      val files = log.update().allFiles.collect()
      assert(files.length === 1, s"expected a single compacted file, got ${files.length}")
      assert(deletionVectorCardinalities(log) === Seq(1L),
        "compacted output should carry the remapped DV for the new delete only")
    }
  }

  test("reconciliation remaps a delete to a non-DV file in a bin that also holds a DV'd file") {
    withTempDir { dir =>
      val log = createMultiFileTable(dir)
      // Pre-existing DV on the middle file (id=100) drops its live count to 99, which shifts the
      // LAST file's output offset. A concurrent delete of id=250 (last file, no DV) then has to
      // land at the shifted offset (199+50, not 200+50): catches an off-by-one in the cumulative
      // offset if physical instead of live counts were used for the DV'd file.
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 100")
      val txnA = sqlTxn(s"OPTIMIZE ${tableRef(dir)}", reconcile = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 250", reconcile = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureA, Duration.Inf)

      assert(ids(dir) === (0L until 300L).filterNot(x => x == 100L || x == 250L))
      val files = log.update().allFiles.collect()
      assert(files.length === 1, s"expected a single compacted file, got ${files.length}")
      assert(deletionVectorCardinalities(log) === Seq(1L),
        "compacted output DV should carry only the concurrent delete of id=250")
    }
  }

  test("reconciliation remaps across multiple read-time DV gaps in one source") {
    withTempDir { dir =>
      val log = createMultiFileTable(dir)
      // Fragment the middle file's read-time DV: delete physical rows 0, 1 and 5 (id 100, 101, 105)
      // before OPTIMIZE reads. The output tag records only the file's 97 live rows (no per-gap
      // segments), so remapping the winner's later delete must reconstruct all three gaps from the
      // read-time DV and discount them from the physical offset. This is the fragmented-DV case the
      // lazy reconstruction is built for: a delete at physical row 50 lands at live-rank 50-3=47.
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id IN (100, 101, 105)")
      assert(deletionVectorCardinalities(log) === Seq(3L), "pre-existing 3-row DV expected")

      val txnA = sqlTxn(s"OPTIMIZE ${tableRef(dir)}", reconcile = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 150", reconcile = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureA, Duration.Inf)

      // If the gap discount were wrong, a different physical row would be masked and a different id
      // would go missing, so the exact surviving set validates the multi-gap rank arithmetic.
      assert(ids(dir) === (0L until 300L).filterNot(x => Set(100L, 101L, 105L, 150L).contains(x)))
      val files = log.update().allFiles.collect()
      assert(files.length === 1, s"expected a single compacted file, got ${files.length}")
      assert(deletionVectorCardinalities(log) === Seq(1L),
        "compacted output DV should carry only the remapped new delete (id=150)")
    }
  }

  test("compaction OPTIMIZE (loser) reconciles a concurrent UPDATE by remapping the DV") {
    withTempDir { dir =>
      val log = createMultiFileTable(dir)
      // UPDATE with DVs masks the old row (a DV on the source file, which the loser remaps onto the
      // compacted output) and appends the new value in a fresh image file. Same remap path as a
      // DELETE, but exercising an UPDATE as the winner.
      val txnA = sqlTxn(s"OPTIMIZE ${tableRef(dir)}", reconcile = true)
      val txnB = sqlTxn(s"UPDATE ${tableRef(dir)} SET id = id + 1000 WHERE id = 150",
        reconcile = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureA, Duration.Inf)

      // Old id=150 masked, new id=1150 present, everything else intact.
      assert(ids(dir) === ((0L until 300L).filterNot(_ == 150L) :+ 1150L).sorted)
      // The compacted output carries the remapped DV (cardinality 1); the winner's fresh image
      // file has none.
      assert(deletionVectorCardinalities(log) === Seq(1L),
        "compacted output should carry the remapped DV for the updated row")
    }
  }

  test("reconciliation targets the correct output among multiple compaction bins") {
    withTempDir { dir =>
      val log = createMultiFileTable(dir, n = 400, files = 4)
      // Force two 2-file bins (maxFileSize = the two largest files) so OPTIMIZE emits two compacted
      // outputs. The remap must land the winner's DV on the output whose bin held the deleted row,
      // computing that output's offsets independently of the other output.
      val sizes = log.update().allFiles.collect().map(_.size).sorted.reverse
      val maxFileSize = (sizes(0) + sizes(1)).toString
      val txnA = sqlTxn(s"OPTIMIZE ${tableRef(dir)}", reconcile = true,
        Seq(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> maxFileSize))
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 150", reconcile = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureA, Duration.Inf)

      assert(ids(dir) === (0L until 400L).filterNot(_ == 150L))
      val files = log.update().allFiles.collect()
      assert(files.length === 2, s"expected two compacted outputs, got ${files.length}")
      // Exactly one of the two outputs carries the remapped DV (the bin that held id=150).
      assert(deletionVectorCardinalities(log) === Seq(1L),
        "only the bin that held the deleted row should carry a remapped DV")
    }
  }

  test("reclustering (ZORDER) OPTIMIZE loser is not reconciled and aborts") {
    withTempDir { dir =>
      createMultiFileTable(dir)
      // ZORDER permutes rows across files, so its output carries no composition tag: the offset
      // remap cannot apply and the loser must abort. Reconciliation must never engage on a
      // row-permuting rewrite, or it would mask the wrong physical rows.
      val e = runOptimizeLoserExpectingAbort(
        dir, s"OPTIMIZE ${tableRef(dir)} ZORDER BY (id)", deleteId = 150)
      assertConcurrentModificationException(e)
      // The winner's delete stands and nothing was corrupted by a bad remap.
      assert(ids(dir) === (0L until 300L).filterNot(_ == 150L))
    }
  }

  test("repartition-path OPTIMIZE loser is not reconciled and aborts") {
    withTempDir { dir =>
      createMultiFileTable(dir)
      // On the repartition path a shuffle reorders rows across files, so no source composition is
      // captured (the operator is not even injected) -> no tag -> the loser aborts (as today).
      val e = runOptimizeLoserExpectingAbort(
        dir, s"OPTIMIZE ${tableRef(dir)}", deleteId = 150,
        extraConf = Seq(DeltaSQLConf.DELTA_OPTIMIZE_REPARTITION_ENABLED.key -> "true"))
      assertConcurrentModificationException(e)
      assert(ids(dir) === (0L until 300L).filterNot(_ == 150L))
    }
  }

  test("reverse direction OFF: a losing DELETE against a compacted file aborts") {
    withTempDir { dir =>
      val log = createMultiFileTable(dir)
      // Reverse reconciliation is a separate opt-in (reverse.enabled). With only the forward flag on,
      // an OPTIMIZE that wins (commits first) against a losing DELETE compacts the delete's target
      // file away, so the loser aborts.
      val txnDelete = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 150", reconcile = true)
      val txnOptimize = sqlTxn(s"OPTIMIZE ${tableRef(dir)}", reconcile = true)

      // A = DELETE (starts first, ends last = loser); B = OPTIMIZE (commits in between = winner).
      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnDelete, txnOptimize)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)
      // OPTIMIZE succeeded and compacted to one file; the delete did not apply.
      assert(ids(dir) === (0L until 300L))
      val files = log.update().allFiles.collect()
      assert(files.length === 1, s"expected a single compacted file, got ${files.length}")
    }
  }

  test("partitioned compaction OPTIMIZE (loser) reconciles a concurrent DELETE") {
    withTempDir { dir =>
      val log = createPartitionedMultiFileTable(dir)
      // The partitioned case F1 fixes: the writer's required ordering is the partition column, so
      // the capture must report that (constant-within-bin) ordering, or a SortExec is inserted above
      // the capture and the recorded offsets become scan order rather than physical write order.
      // End-to-end this reconciles like the unpartitioned case: both commit, exactly id=150 gone.
      val txnA = sqlTxn(s"OPTIMIZE ${tableRef(dir)}", reconcile = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 150", reconcile = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureA, Duration.Inf)

      assert(ids(dir) === (0L until 300L).filterNot(_ == 150L))
      val files = log.update().allFiles.collect()
      assert(files.length === 1, s"expected a single compacted file, got ${files.length}")
      assert(deletionVectorCardinalities(log) === Seq(1L),
        "compacted output should carry the remapped deletion vector")
    }
  }

  test("partitioned OPTIMIZE capture reports the partition ordering (no sort above the capture)") {
    withTempDir { dir =>
      createPartitionedMultiFileTable(dir)
      // Deterministic, structural proof of the sort-skip. The behavioral partitioned test above is
      // non-deterministic (equal partition keys in a bin + an unstable sort); this single-bin table
      // compacts to exactly one file, so the executed write plan is deterministic. With the fix the
      // capture reports the constant partition-column ordering, the writer's required ordering is
      // satisfied, and no SortExec is inserted above the capture, so the recorded offsets are the
      // physical write order. We assert on the captured plan directly rather than on the indirect
      // `outputOrderingMatched` proxy (which passes vacuously when the required ordering is empty,
      // and never checks the capture is present).
      DeltaFileFormatWriter.executedPlan = None
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_CONFLICT_RECONCILIATION_ENABLED.key -> "true") {
        sql(s"OPTIMIZE ${tableRef(dir)}").collect()
      }
      val plan = DeltaFileFormatWriter.executedPlan.getOrElse(
        fail("no executed write plan was captured for the partitioned OPTIMIZE"))
      // The capture must be in the write plan at all, else the no-sort assertion below is vacuous.
      assert(plan.exists(_.isInstanceOf[SourceCompositionCaptureExec]),
        s"expected a SourceCompositionCaptureExec in the write plan, got:\n${plan.treeString}")
      // No SortExec may sit above the capture: a sort there would reorder rows after the capture
      // recorded them, turning the write-order offsets into scan order.
      val sortAboveCapture = plan.exists {
        case s: SortExec => s.exists(_.isInstanceOf[SourceCompositionCaptureExec])
        case _ => false
      }
      assert(!sortAboveCapture,
        s"a SortExec was inserted above the capture, corrupting write-order offsets:\n" +
          plan.treeString)
      // Complementary: the writer must also have found the ordering matched (no sort needed).
      assert(DeltaFileFormatWriter.outputOrderingMatched,
        "a partitioned OPTIMIZE capture write must not require a sort above the capture operator")
    }
  }

  // ---------------------------------------------------------------------------
  // Reverse direction (optimize.conflictReconciliation.reverse.enabled): the DML LOSES to a
  // compaction OPTIMIZE and remaps its deletion vector onto the winner's compacted output.
  // ---------------------------------------------------------------------------

  private val reverseKey =
    DeltaSQLConf.DELTA_OPTIMIZE_CONFLICT_RECONCILIATION_REVERSE_ENABLED.key

  /** A transaction with reverse reconciliation enabled (needed on both the OPTIMIZE and the DML). */
  private def reverseTxn(sqlText: String, extraConf: Seq[(String, String)] = Nil): () => Array[Row] =
    sqlTxn(sqlText, reconcile = false, (reverseKey -> "true") +: extraConf)

  test("reverse: a losing DELETE remaps its DV onto the winning compaction output") {
    withTempDir { dir =>
      val log = createMultiFileTable(dir)
      // A (loser): DELETE id=150. B (winner): OPTIMIZE compacts all files (persisting composition).
      // A's target file is compacted away; instead of aborting, A remaps its delete onto the output.
      val txnDelete = reverseTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 150")
      val txnOptimize = reverseTxn(s"OPTIMIZE ${tableRef(dir)}")

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnDelete, txnOptimize)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureA, Duration.Inf)

      // Both committed: id=150 gone, everything else present, compacted to one file carrying the
      // remapped DV (cardinality 1) -- proving the DELETE reconciled rather than aborting.
      assert(ids(dir) === (0L until 300L).filterNot(_ == 150L))
      val files = log.update().allFiles.collect()
      assert(files.length === 1, s"expected a single compacted file, got ${files.length}")
      assert(deletionVectorCardinalities(log) === Seq(1L),
        "compacted output should carry the losing delete's remapped DV")
    }
  }

  test("reverse: losing DELETE remaps correctly across an OPTIMIZE-time read gap") {
    withTempDir { dir =>
      val log = createMultiFileTable(dir)
      // Pre-existing DV: id=100 (physical row 0 of the middle file) is deleted before either txn, so
      // the OPTIMIZE excludes it from the output and the middle file's live rows start at physical
      // index 1. The losing delete of id=150 (physical row 50) must land at live-rank 50-1=49 plus
      // the first file's 100 rows -> output position 149, discounting the OPTIMIZE-time gap.
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 100")
      assert(deletionVectorCardinalities(log) === Seq(1L), "pre-existing DV expected on one file")

      val txnDelete = reverseTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 150")
      val txnOptimize = reverseTxn(s"OPTIMIZE ${tableRef(dir)}")

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnDelete, txnOptimize)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureA, Duration.Inf)

      // If the gap discount were wrong, a different physical row would be masked and a different id
      // would go missing, so the exact surviving set validates the remap arithmetic.
      assert(ids(dir) === (0L until 300L).filterNot(x => x == 100L || x == 150L))
      val files = log.update().allFiles.collect()
      assert(files.length === 1, s"expected a single compacted file, got ${files.length}")
      // The pre-existing id=100 was purged by the compaction (not in the output); the output DV
      // carries only the losing delete of id=150.
      assert(deletionVectorCardinalities(log) === Seq(1L),
        "compacted output DV should carry only the remapped losing delete (id=150)")
    }
  }

  test("reverse: a losing UPDATE remaps its DV onto the winning compaction output") {
    withTempDir { dir =>
      val log = createMultiFileTable(dir)
      // The UPDATE masks the old row (a DV, which is remapped onto the output) and appends the new
      // value in a fresh image file (a new AddFile the OPTIMIZE never touched, so it does not
      // conflict). A (loser): UPDATE; B (winner): OPTIMIZE.
      val txnUpdate = reverseTxn(s"UPDATE ${tableRef(dir)} SET id = id + 1000 WHERE id = 150")
      val txnOptimize = reverseTxn(s"OPTIMIZE ${tableRef(dir)}")

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnUpdate, txnOptimize)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureA, Duration.Inf)

      // Old id=150 masked, new id=1150 present, everything else intact.
      assert(ids(dir) === ((0L until 300L).filterNot(_ == 150L) :+ 1150L).sorted)
      // The compacted output carries the remapped DV (cardinality 1); the fresh image file has none.
      assert(deletionVectorCardinalities(log) === Seq(1L),
        "compacted output should carry the remapped DV for the updated row")
    }
  }

  test("reverse: aborts when the winning OPTIMIZE recorded no composition") {
    withTempDir { dir =>
      val log = createMultiFileTable(dir)
      // The winning OPTIMIZE runs with reconciliation disabled in BOTH directions, so it captures
      // and persists no source composition (mirroring a writer/engine that doesn't record it). The
      // losing DELETE finds no tag on the compacted-away file and aborts -- reconciliation is
      // reader-optional and abort-safe. The two txns share one SparkSession, so the OPTIMIZE pins
      // both flags OFF explicitly, otherwise the DML's flags would leak onto its conf read.
      val txnDelete = reverseTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 150")
      val txnOptimize = sqlTxn(s"OPTIMIZE ${tableRef(dir)}", reconcile = false,
        extraConf = Seq(reverseKey -> "false"))

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnDelete, txnOptimize)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)
      assert(ids(dir) === (0L until 300L))
      assert(log.update().allFiles.collect().length === 1)
    }
  }

  test("reverse: aborts against a reclustering (ZORDER) winner") {
    withTempDir { dir =>
      val log = createMultiFileTable(dir)
      // ZORDER permutes rows, so the OPTIMIZE captures no composition even with the reverse flag on;
      // the losing DML cannot remap (no offset mapping exists) and aborts.
      val txnDelete = reverseTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 150")
      val txnOptimize = reverseTxn(s"OPTIMIZE ${tableRef(dir)} ZORDER BY (id)")

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnDelete, txnOptimize)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)
      assert(ids(dir) === (0L until 300L))
    }
  }

  test("reverse: aborts when a losing full-file delete cannot be remapped (mixed full/partial)") {
    withTempDir { dir =>
      val log = createMultiFileTable(dir)
      // The losing DML both FULLY deletes one source file (id < 100 -> a bare RemoveFile with no
      // re-added DV'd AddFile) and PARTIALLY deletes another (id = 150 -> RemoveFile + DV'd
      // AddFile). The winning OPTIMIZE compacts all three files away. The partial delete is
      // remappable, but the full delete has no DV to remap onto the output; reconciling only the
      // partial one while blanket-resolving every compaction source would silently drop the full
      // delete (ids 0-99 would resurface). H1 forces an abort: no DML-touched winner-removed source
      // may be left un-remapped.
      val txnDelete = reverseTxn(s"DELETE FROM ${tableRef(dir)} WHERE id < 100 OR id = 150")
      val txnOptimize = reverseTxn(s"OPTIMIZE ${tableRef(dir)}")

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnDelete, txnOptimize)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)
      // The DML aborted, so the winner's compaction stands unchanged: every row still present in
      // the single compacted output, no rows lost.
      assert(ids(dir) === (0L until 300L))
      assert(log.update().allFiles.collect().length === 1)
    }
  }

  // --- Composition-tag parser ---
  // Both reconcile directions read the source composition through the shared parser
  // `ConflictChecker.parseOptimizeSourceComposition`, which decodes the `compactedInto` /
  // `compactionInfo` tags (a format modeled on Databricks Runtime's; cross-engine reconciliation is
  // best-effort). These unit-test the parser directly; the end-to-end remap that consumes its
  // `(outputPath, outputStart, liveCount)` output is covered by the tests above.

  private def removeWithTags(tags: Map[String, String], dvCardinality: Long = 0L): RemoveFile = {
    val dv =
      if (dvCardinality > 0L) DeletionVectorDescriptor.EMPTY.copy(cardinality = dvCardinality)
      else null
    RemoveFile("src.parquet", Some(1L), deletionVector = dv, tags = tags)
  }

  private val outputs = Set("out.parquet")

  test("composition tag: parses a single-run compaction entry") {
    // No source DV, so sourceNumPhysicalRecords (90) == liveCount.
    val r = removeWithTags(Map(
      "compactedInto" -> """["out.parquet"]""",
      "compactionInfo" -> """[{"rowOffsetInTarget":100,"sourceNumPhysicalRecords":90}]"""))
    assert(ConflictChecker.parseOptimizeSourceComposition(r, outputs) ===
      Some(("out.parquet", 100L, 90L)))
  }

  test("composition tag: derives liveCount from the physical count minus the read-time DV") {
    // physical 100, read-time DV cardinality 10 on the same tombstone -> liveCount 90.
    val r = removeWithTags(
      Map(
        "compactedInto" -> """["out.parquet"]""",
        "compactionInfo" -> """[{"rowOffsetInTarget":100,"sourceNumPhysicalRecords":100}]"""),
      dvCardinality = 10L)
    assert(ConflictChecker.parseOptimizeSourceComposition(r, outputs) ===
      Some(("out.parquet", 100L, 90L)))
  }

  test("composition tag: tolerates unknown fields in a compaction entry (schema drift)") {
    val info =
      """[{"rowOffsetInTarget":100,"sourceNumPhysicalRecords":90,"sourceDeletionVector":null}]"""
    val r = removeWithTags(Map(
      "compactedInto" -> """["out.parquet"]""",
      "compactionInfo" -> info))
    assert(ConflictChecker.parseOptimizeSourceComposition(r, outputs) ===
      Some(("out.parquet", 100L, 90L)))
  }

  test("composition tag: falls back (None) on shapes the contiguous offset remap cannot model") {
    // Output not among this commit's added files.
    assert(ConflictChecker.parseOptimizeSourceComposition(
      removeWithTags(Map(
        "compactedInto" -> """["elsewhere.parquet"]""",
        "compactionInfo" -> """[{"rowOffsetInTarget":0,"sourceNumPhysicalRecords":90}]""")),
      outputs).isEmpty)
    // Source split across more than one output run (multi-entry) -- not modeled.
    assert(ConflictChecker.parseOptimizeSourceComposition(
      removeWithTags(Map(
        "compactedInto" -> """["out.parquet"]""",
        "compactionInfo" ->
          ("""[{"rowOffsetInTarget":0,"sourceNumPhysicalRecords":40},""" +
            """{"rowOffsetInTarget":40,"sourceNumPhysicalRecords":50}]"""))),
      outputs).isEmpty)
    // Malformed JSON.
    assert(ConflictChecker.parseOptimizeSourceComposition(
      removeWithTags(Map("compactedInto" -> """["out.parquet"]""", "compactionInfo" -> "not json")),
      outputs).isEmpty)
    // No composition tags at all.
    assert(
      ConflictChecker.parseOptimizeSourceComposition(removeWithTags(Map.empty), outputs).isEmpty)
  }
}
