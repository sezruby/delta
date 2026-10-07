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
import scala.util.control.NonFatal

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.concurrency.{PhaseLockingTestMixin, TransactionExecutionTestMixin}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

/**
 * Tests for [[DeltaSQLConf.MERGE_SOURCE_KEY_READ_PREDICATE_ENABLED]]: a MERGE records the target
 * rows whose key matches a source key as its read predicate (a pre-filter plus an exact semi-join
 * against the MERGE source, like DBR) so that conflict-time data skipping can reconcile concurrent
 * MERGEs on disjoint keys, however many keys there are, while concurrent changes to the same key
 * (an update of the same row, or an insert of the same new key) still conflict.
 *
 * Each race runs the loser MERGE up to pre-commit, commits the winner MERGE, then lets the loser
 * commit. The table has two files, ids [0, 50) and [50, 100), so that two MERGEs that update
 * different existing rows touch different files and the scenario does not depend on same-file
 * deletion-vector reconciliation.
 */
class MergeSourceKeyReadPredicateSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with PhaseLockingTestMixin
  with TransactionExecutionTestMixin {

  private val COMMITTED = "committed"

  private def tableRef(dir: File): String = s"delta.`${dir.getCanonicalPath}`"

  private def createTable(dir: File, deletionVectors: Boolean): Unit = {
    val path = dir.getCanonicalPath
    spark.range(0, 50).selectExpr("id", "0 AS v").repartition(1)
      .write.format("delta").save(path)
    spark.range(50, 100).selectExpr("id", "0 AS v").repartition(1)
      .write.format("delta").mode("append").save(path)
    sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
      s"('delta.enableDeletionVectors' = '$deletionVectors')")
  }

  private def upsert(dir: File, keys: Seq[Long], v: Int): String = {
    val values = keys.map(k => s"(${k}L, $v)").mkString(", ")
    s"""MERGE INTO ${tableRef(dir)} t
       |USING (SELECT * FROM VALUES $values AS s(id, v)) s
       |ON t.id = s.id
       |WHEN MATCHED THEN UPDATE SET v = s.v
       |WHEN NOT MATCHED THEN INSERT *""".stripMargin
  }

  private def insertOnly(dir: File, keys: Seq[Long], v: Int): String = {
    val values = keys.map(k => s"(${k}L, $v)").mkString(", ")
    s"""MERGE INTO ${tableRef(dir)} t
       |USING (SELECT * FROM VALUES $values AS s(id, v)) s
       |ON t.id = s.id
       |WHEN NOT MATCHED THEN INSERT *""".stripMargin
  }

  private def allConfs(
      enabled: Boolean,
      inSetThreshold: Int = 10000): Seq[(String, String)] = Seq(
    DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_ENABLED.key -> "true",
    DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_VALUE_EXACT_ENABLED.key -> "true",
    DeltaSQLConf.MERGE_SOURCE_KEY_READ_PREDICATE_ENABLED.key -> enabled.toString,
    DeltaSQLConf.MERGE_SOURCE_KEY_READ_PREDICATE_IN_SET_THRESHOLD.key -> inSetThreshold.toString)

  /**
   * Runs `loserSql` up to pre-commit, commits `winnerSql`, then lets the loser commit. Returns
   * [[COMMITTED]] or the simple class name of the concurrent-modification exception.
   */
  private def race(loserSql: String, winnerSql: String): String = {
    val pool = ThreadUtils.newDaemonSingleThreadExecutor(threadName = "merge-loser")
    try {
      val (observer, future) = runQueryWithObserver(name = "loser", pool, loserSql)
      unblockUntilPreCommit(observer)
      busyWaitFor(observer.phases.preparePhase.hasEntered, timeout)
      sql(winnerSql)
      unblockCommit(observer)
      try {
        ThreadUtils.awaitResult(future, Duration.Inf)
        COMMITTED
      } catch {
        case NonFatal(e) =>
          Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null)
            .collectFirst { case c: DeltaConcurrentModificationException => c }
            .map(_.getClass.getSimpleName)
            .getOrElse(throw e)
      }
    } finally {
      pool.shutdownNow()
    }
  }

  private def ids(dir: File): Seq[Long] =
    sql(s"SELECT id FROM ${tableRef(dir)}").collect().map(_.getLong(0)).toSeq

  for (dv <- Seq(true, false)) {
    test(s"disjoint keys: loser commits, both MERGEs applied (deletionVectors=$dv)") {
      withTempDir { dir =>
        createTable(dir, deletionVectors = dv)
        withSQLConf(allConfs(enabled = true): _*) {
          // Winner updates 10 (file [0, 50)) and inserts 1000: its new file's id range [10, 1000]
          // spans the loser's keys, so only the value-exact check can prove no conflict.
          assert(race(upsert(dir, Seq(60L, 500L), 2), upsert(dir, Seq(10L, 1000L), 1)) ==
            COMMITTED)
        }
        checkAnswer(
          sql(s"SELECT id, v FROM ${tableRef(dir)} WHERE id IN (10, 60, 500, 1000)"),
          Seq(Row(10L, 1), Row(60L, 2), Row(500L, 2), Row(1000L, 1)))
        val all = ids(dir)
        assert(all.size == 102 && all.distinct.size == 102, "no lost or duplicated rows")
      }
    }
  }

  test("feature off: disjoint keys still conflict (key-only ON reads the whole table)") {
    withTempDir { dir =>
      createTable(dir, deletionVectors = true)
      withSQLConf(allConfs(enabled = false): _*) {
        assert(race(upsert(dir, Seq(60L, 500L), 2), upsert(dir, Seq(10L, 1000L), 1)) ==
          "ConcurrentAppendException")
      }
    }
  }

  test("same new key inserted by both MERGEs: loser aborts, no duplicate key") {
    withTempDir { dir =>
      createTable(dir, deletionVectors = true)
      withSQLConf(allConfs(enabled = true): _*) {
        assert(race(upsert(dir, Seq(60L, 500L), 2), upsert(dir, Seq(10L, 500L), 1)) ==
          "ConcurrentAppendException")
      }
      assert(ids(dir).count(_ == 500L) == 1, "the new key must be inserted exactly once")
    }
  }

  test("same existing key updated by both MERGEs: loser aborts") {
    withTempDir { dir =>
      createTable(dir, deletionVectors = true)
      withSQLConf(allConfs(enabled = true): _*) {
        val outcome = race(upsert(dir, Seq(60L), 2), upsert(dir, Seq(60L), 1))
        assert(outcome != COMMITTED, "updating the same row concurrently must conflict")
      }
      checkAnswer(sql(s"SELECT v FROM ${tableRef(dir)} WHERE id = 60"), Row(1))
    }
  }

  test("more source keys than inSetThreshold: range pre-filter, the exact key match decides") {
    withTempDir { dir =>
      createTable(dir, deletionVectors = true)
      // 20000 even keys in [1000, 40998]: above the default threshold, so the pre-filter is the
      // range [1000, 40998], which every winner key below falls into.
      val loser =
        s"""MERGE INTO ${tableRef(dir)} t
           |USING (SELECT 1000 + 2 * id AS id, 2 AS v FROM range(20000)) s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET v = s.v
           |WHEN NOT MATCHED THEN INSERT *""".stripMargin
      withSQLConf(allConfs(enabled = true): _*) {
        val events = sourceKeyEvents {
          assert(race(loser, upsert(dir, Seq(10L, 1001L), 1)) == COMMITTED)
        }
        assert(events.exists(_("preFilter") == "range"))
        // Control: the winner updates one of the loser's 20000 keys.
        assert(race(loser.replace("2 AS v", "3 AS v"), upsert(dir, Seq(1002L), 1)) != COMMITTED)
      }
      val all = ids(dir)
      assert(all.size == 100 + 20000 + 1 && all.distinct.size == all.size)
    }
  }

  test("tiny inSetThreshold only changes the pre-filter: disjoint keys still commit") {
    withTempDir { dir =>
      createTable(dir, deletionVectors = true)
      for (threshold <- Seq(1, 0)) {
        withSQLConf(allConfs(enabled = true, inSetThreshold = threshold): _*) {
          // The winner's new key 300 is inside the loser's key range [60, 500 + threshold].
          val loserKeys = Seq(60L, 500L + threshold)
          assert(race(upsert(dir, loserKeys, 2), upsert(dir, Seq(10L, 300L + threshold), 1)) ==
            COMMITTED)
        }
      }
      assert(ids(dir).size == 100 + 4)
    }
  }

  test("multi-column key: tuples are matched exactly, not per column") {
    withTempDir { dir =>
      createTable(dir, deletionVectors = true)
      // The loser reads (id, v) in {(60, 0), (61, 5)}. The winner's (60, 5) passes the per-column
      // pre-filters (id IN (60, 61), v IN (0, 5)) but is not one of the loser's key tuples.
      val loser =
        s"""MERGE INTO ${tableRef(dir)} t
           |USING (SELECT * FROM VALUES (60L, 0, 7), (61L, 5, 7) AS s(id, v, nv)) s
           |ON t.id = s.id AND t.v = s.v
           |WHEN MATCHED THEN UPDATE SET v = s.nv
           |WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)""".stripMargin
      withSQLConf(allConfs(enabled = true): _*) {
        assert(race(loser, s"INSERT INTO ${tableRef(dir)} VALUES (60, 5)") == COMMITTED)
      }
      checkAnswer(sql(s"SELECT id, v FROM ${tableRef(dir)} WHERE id IN (60, 61)"),
        Seq(Row(60L, 7), Row(60L, 5), Row(61L, 0), Row(61L, 5)))
    }
  }

  test("insert-only MERGE: disjoint new keys commit, the same new key aborts") {
    withTempDir { dir =>
      createTable(dir, deletionVectors = true)
      withSQLConf(allConfs(enabled = true): _*) {
        assert(race(insertOnly(dir, Seq(500L), 2), insertOnly(dir, Seq(1000L, 10L), 1)) ==
          COMMITTED)
        assert(race(insertOnly(dir, Seq(700L), 2), insertOnly(dir, Seq(2000L, 700L), 1)) ==
          "ConcurrentAppendException")
      }
      val all = ids(dir)
      assert(all.count(_ == 500L) == 1 && all.count(_ == 1000L) == 1 && all.count(_ == 700L) == 1)
    }
  }

  test("NOT MATCHED BY SOURCE reads every target row: no source-key predicate, conflicts") {
    withTempDir { dir =>
      createTable(dir, deletionVectors = true)
      val loser =
        s"""MERGE INTO ${tableRef(dir)} t
           |USING (SELECT * FROM VALUES (60L, 2) AS s(id, v)) s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET v = s.v
           |WHEN NOT MATCHED BY SOURCE AND t.id = 1000 THEN DELETE""".stripMargin
      withSQLConf(allConfs(enabled = true): _*) {
        assert(race(loser, upsert(dir, Seq(10L, 1000L), 1)) != COMMITTED)
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Predicate construction, observed through the usage event.
  // ---------------------------------------------------------------------------------------------

  private def sourceKeyEvents(f: => Unit): Seq[Map[String, Any]] =
    Log4jUsageLogger.track(f)
      .filter(_.tags.get("opType").contains("delta.dml.merge.sourceKeyReadPredicate"))
      .map(e => JsonUtils.fromJson[Map[String, Any]](e.blob))

  test("event: IN pre-filter within inSetThreshold, range above, keys de-duplicated") {
    withTempDir { dir =>
      createTable(dir, deletionVectors = true)
      withSQLConf(allConfs(enabled = true, inSetThreshold = 2): _*) {
        // Duplicate source keys (existing target rows, so insert-only MERGE is a no-op).
        val within = sourceKeyEvents(sql(insertOnly(dir, Seq(1L, 2L, 2L), 5)))
        assert(within.size == 1)
        assert(within.head("preFilter") == "inSet" && within.head("numDistinctKeys") == 2)

        val above = sourceKeyEvents(sql(upsert(dir, Seq(1L, 2L, 3L), 6)))
        assert(above.size == 1)
        assert(above.head("preFilter") == "range" && above.head("numDistinctKeys") == -1)

        // A NULL key matches no target row: the MERGE reads nothing and updates nothing.
        val nullOnly = sourceKeyEvents(sql(
          s"""MERGE INTO ${tableRef(dir)} t
             |USING (SELECT CAST(NULL AS BIGINT) AS id, 7 AS v) s
             |ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET v = s.v""".stripMargin))
        assert(nullOnly.size == 1)
      }
      checkAnswer(sql(s"SELECT id, v FROM ${tableRef(dir)} WHERE id IN (1, 2, 3)"),
        Seq(Row(1L, 6), Row(2L, 6), Row(3L, 6)))
    }
  }

  test("event: no equi-join key between target and source -> not attempted") {
    withTempDir { dir =>
      createTable(dir, deletionVectors = true)
      withSQLConf(allConfs(enabled = true): _*) {
        val events = sourceKeyEvents(sql(
          s"""MERGE INTO ${tableRef(dir)} t
             |USING (SELECT 60L AS id, 9 AS v) s
             |ON t.id < s.id AND t.id >= 59
             |WHEN MATCHED THEN UPDATE SET v = s.v""".stripMargin))
        assert(events.isEmpty)
      }
      checkAnswer(sql(s"SELECT v FROM ${tableRef(dir)} WHERE id = 59"), Row(9))
    }
  }

  test("event: multi-column key, MERGE result unchanged") {
    withTempDir { dir =>
      createTable(dir, deletionVectors = true)
      withSQLConf(allConfs(enabled = true): _*) {
        val events = sourceKeyEvents(sql(
          s"""MERGE INTO ${tableRef(dir)} t
             |USING (SELECT * FROM VALUES (60L, 0, 3), (61L, 0, 3), (62L, 1, 3) AS s(id, v, nv)) s
             |ON t.id = s.id AND t.v = s.v
             |WHEN MATCHED THEN UPDATE SET v = s.nv""".stripMargin))
        assert(events.size == 1)
        assert(events.head("preFilter") == "inSet" && events.head("numKeyColumns") == 2)
      }
      checkAnswer(sql(s"SELECT id, v FROM ${tableRef(dir)} WHERE id IN (60, 61, 62)"),
        Seq(Row(60L, 3), Row(61L, 3), Row(62L, 0)))
    }
  }
}
