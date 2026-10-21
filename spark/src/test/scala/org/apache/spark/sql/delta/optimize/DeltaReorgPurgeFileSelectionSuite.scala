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

package org.apache.spark.sql.delta.optimize

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.delta.{
  DeletionVectorsTestUtils,
  DeltaLog,
  DeltaOperations,
  DeltaTestUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.util.DeltaFileOperations

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class DeltaReorgPurgeFileSelectionSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaSQLTestUtils
  with DeletionVectorsTestUtils {

  private val ratioKey = DeltaSQLConf.DELTA_REORG_PURGE_MIN_DELETED_ROWS_RATIO.key
  private val stableKey = DeltaSQLConf.DELTA_REORG_PURGE_MIN_STABLE_DURATION.key
  private val maxAgeKey = DeltaSQLConf.DELTA_REORG_PURGE_MAX_DELETION_VECTOR_AGE.key

  private val day = TimeUnit.DAYS.toMillis(1)

  /**
   * One file per partition, each with a deletion vector:
   *  - a: 50% deleted, DV added 10 days ago
   *  - b: 1% deleted, DV added 10 days ago
   *  - c: 50% deleted, DV added 1 hour ago
   */
  private def withDVTable(f: (String, DeltaLog) => Unit): Unit = withTempDir { dir =>
    val path = dir.getCanonicalPath
    sql(s"""CREATE TABLE delta.`$path` (id INT, p STRING) USING delta PARTITIONED BY (p)
           |TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""".stripMargin)
    Seq("a", "b", "c").foreach { p =>
      spark.range(0, 100).selectExpr("CAST(id AS INT) AS id", s"'$p' AS p").coalesce(1)
        .write.format("delta").mode("append").save(path)
    }
    sql(s"DELETE FROM delta.`$path` WHERE p = 'a' AND id < 50")
    sql(s"DELETE FROM delta.`$path` WHERE p = 'b' AND id = 0")
    sql(s"DELETE FROM delta.`$path` WHERE p = 'c' AND id < 50")

    val log = DeltaLog.forTable(spark, path)
    val latest = log.update().version
    assert(latest === 6L)
    val now = System.currentTimeMillis()
    (0L until latest).foreach { v =>
      DeltaTestUtils.modifyCommitTimestamp(log, v, now - 10 * day + v * 1000L)
    }
    DeltaTestUtils.modifyCommitTimestamp(log, latest, now - TimeUnit.HOURS.toMillis(1))
    DeltaLog.clearCache()
    val freshLog = DeltaLog.forTable(spark, path)
    assert(dvFilesByPartition(freshLog).keySet === Set("a", "b", "c"))
    f(path, freshLog)
  }

  private def dvFilesByPartition(log: DeltaLog): Map[String, String] =
    log.update().allFiles.collect()
      .filter(_.deletionVector != null)
      .map(f => f.partitionValues("p") -> f.path)
      .toMap

  /** Runs REORG PURGE and returns the partitions whose DV file was rewritten. */
  private def purge(path: String, log: DeltaLog, confs: (String, String)*): Set[String] = {
    val before = dvFilesByPartition(log)
    withSQLConf(confs: _*) {
      sql(s"REORG TABLE delta.`$path` APPLY (PURGE)")
    }
    val after = log.update().allFiles.collect().map(_.path).toSet
    checkAnswer(sql(s"SELECT COUNT(*) FROM delta.`$path`"), Seq(org.apache.spark.sql.Row(199L)))
    before.collect { case (p, file) if !after.contains(file) => p }.toSet
  }

  test("default confs purge every file with deletion vectors") {
    withDVTable { (path, log) =>
      assert(purge(path, log) === Set("a", "b", "c"))
      assert(dvFilesByPartition(log).isEmpty)
    }
  }

  test("minDeletedRowsRatio skips files with few deleted rows") {
    withDVTable { (path, log) =>
      assert(purge(path, log, ratioKey -> "0.1") === Set("a", "c"))
      assert(dvFilesByPartition(log).keySet === Set("b"))
    }
  }

  test("minStableDuration skips files that recently got a deletion vector") {
    withDVTable { (path, log) =>
      assert(purge(path, log, stableKey -> "7d") === Set("a", "b"))
      assert(dvFilesByPartition(log).keySet === Set("c"))
    }
  }

  test("ratio and stability filters combine") {
    withDVTable { (path, log) =>
      assert(purge(path, log, ratioKey -> "0.1", stableKey -> "7d") === Set("a"))
    }
  }

  test("minDeletedRowsRatio is inclusive at the exact threshold") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 100).coalesce(1).write.format("delta")
        .option("delta.enableDeletionVectors", "true").save(path)
      sql(s"DELETE FROM delta.`$path` WHERE id < 10")
      val log = DeltaLog.forTable(spark, path)
      assert(log.update().allFiles.collect().exists(_.deletionVector != null))
      withSQLConf(ratioKey -> "0.1") {
        sql(s"REORG TABLE delta.`$path` APPLY (PURGE)")
      }
      assert(!log.update().allFiles.collect().exists(_.deletionVector != null))
    }
  }

  test("recent deletion vectors are detected when the commit uses an absolute path") {
    withDVTable { (path, log) =>
      val fileA = log.update().allFiles.collect()
        .find(f => f.partitionValues("p") == "a" && f.deletionVector != null).get
      val absolutePath =
        DeltaFileOperations.absolutePath(log.dataPath.toString, fileA.path).toUri.getRawPath
      assert(absolutePath.startsWith("/"))
      log.startTransaction().commit(
        Seq(fileA.removeWithTimestamp(), fileA.copy(path = absolutePath, dataChange = true)),
        DeltaOperations.ManualUpdate)
      // The snapshot qualifies the path (file:/...) while the commit holds the raw absolute path.
      assert(dvFilesByPartition(log)("a") !== absolutePath)
      // `a` got a DV in the latest commit through an unqualified absolute path, so it is recent.
      assert(purge(path, log, stableKey -> "7d") === Set("b"))
    }
  }

  test("maxDeletionVectorAge purges old deletion vectors regardless of the filters") {
    withDVTable { (path, log) =>
      assert(purge(path, log, ratioKey -> "0.9", stableKey -> "30d", maxAgeKey -> "5d") ===
        Set("a", "b"))
      assert(dvFilesByPartition(log).keySet === Set("c"))
    }
  }

  test("maxDeletionVectorAge alone does not filter anything") {
    withDVTable { (path, log) =>
      assert(purge(path, log, maxAgeKey -> "5d") === Set("a", "b", "c"))
    }
  }

  test("stability window covering the whole table history skips all recently changed files") {
    withDVTable { (path, log) =>
      // The window starts before the table was created; every DV is within it.
      assert(purge(path, log, stableKey -> "365d").isEmpty)
      assert(dvFilesByPartition(log).keySet === Set("a", "b", "c"))
    }
  }

  test("dropping the deletionVectors feature ignores the file selection confs") {
    withDVTable { (path, log) =>
      withSQLConf(ratioKey -> "0.9", stableKey -> "365d") {
        // Depending on the (backdated) history, the drop either completes or purges all DVs and
        // then asks to wait for the retention period. Either way, no DV may remain.
        scala.util.Try(sql(s"ALTER TABLE delta.`$path` DROP FEATURE deletionVectors"))
      }
      assert(dvFilesByPartition(log).isEmpty)
    }
  }
}
