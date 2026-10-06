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

import java.sql.Date

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog}
import org.apache.spark.sql.delta.commands.optimize.StatsBasedFileOrdering
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaStatistics.{MAX, MIN}
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class OptimizeCompactionBinningColumnsSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaSQLTestUtils {

  private val binningKey = DeltaConfigs.COMPACTION_BINNING_COLUMNS.key
  private val startDate = Date.valueOf("2026-01-01")

  // One file is written per day. The row counts make the size order interleave the days
  // (0, 2, 4, 6, 1, 3, 5, 7), so the default size-based binning mixes non-adjacent days.
  private val rowsPerDay = Seq(0, 4, 1, 5, 2, 6, 3, 7).map(1000 + 50 * _)

  private def createTable(
      path: String,
      binningColumns: Option[String],
      columnMappingMode: String = "none",
      extraProperties: Map[String, String] = Map.empty): Unit = {
    val properties = Map("delta.columnMapping.mode" -> columnMappingMode) ++
      binningColumns.map(binningKey -> _) ++ extraProperties
    val props = properties.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")
    sql(
      s"""CREATE TABLE delta.`$path` (id BIGINT, d DATE, n STRUCT<d: DATE>, s STRING)
         |USING delta TBLPROPERTIES ($props)""".stripMargin)
  }

  private def appendOneFilePerDay(path: String): Unit = {
    rowsPerDay.zipWithIndex.foreach { case (rows, day) =>
      spark.range(rows)
        .selectExpr(
          "id",
          s"date_add(date'$startDate', $day) AS d",
          s"named_struct('d', date_add(date'$startDate', $day)) AS n",
          "cast(id AS STRING) AS s")
        .coalesce(1)
        .write.format("delta").mode("append").save(path)
    }
  }

  /** Bins hold exactly two files: any two files fit, no three files do. */
  private def maxFileSizeForTwoFileBins(path: String): Long = {
    val sizes = DeltaLog.forTable(spark, path).update().allFiles.collect().map(_.size).sorted
    assert(sizes.take(3).sum > sizes.takeRight(2).sum)
    sizes.takeRight(2).sum
  }

  /** The (min, max) day of the column `d` of every file in the table. */
  private def dayRanges(path: String): Set[(Int, Int)] = {
    val snapshot = DeltaLog.forTable(spark, path).update()
    def day(date: Date): Int =
      ((date.toLocalDate.toEpochDay - startDate.toLocalDate.toEpochDay)).toInt
    snapshot.withStats
      .select(
        snapshot.getStatsColumnOrNullLiteral(MIN, Seq("d")),
        snapshot.getStatsColumnOrNullLiteral(MAX, Seq("d")))
      .collect()
      .map(r => (day(r.getDate(0)), day(r.getDate(1))))
      .toSet
  }

  private def optimizeWithTwoFileBins(path: String): Unit = {
    val maxFileSize = maxFileSizeForTwoFileBins(path)
    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> maxFileSize.toString) {
      sql(s"OPTIMIZE delta.`$path`")
    }
  }

  private def checkData(path: String): Unit = {
    val expected = rowsPerDay.zipWithIndex.flatMap { case (rows, day) =>
      (0 until rows).map { id =>
        val date = Date.valueOf(startDate.toLocalDate.plusDays(day))
        Row(id.toLong, date, Row(date), id.toString)
      }
    }
    checkAnswer(spark.read.format("delta").load(path), expected)
  }

  test("without binning columns, files are grouped by size") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createTable(path, binningColumns = None)
      appendOneFilePerDay(path)
      optimizeWithTwoFileBins(path)
      assert(dayRanges(path) === Set((0, 2), (4, 6), (1, 3), (5, 7)))
      checkData(path)
    }
  }

  for {
    binningColumns <- Seq("d", "n.d", "`d`, s")
    columnMappingMode <- Seq("none", "name")
  } test(s"files are grouped by the stats of the binning columns - " +
      s"binningColumns: $binningColumns, columnMapping: $columnMappingMode") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createTable(path, Some(binningColumns), columnMappingMode)
      appendOneFilePerDay(path)
      optimizeWithTwoFileBins(path)
      assert(dayRanges(path) === Set((0, 1), (2, 3), (4, 5), (6, 7)))
      checkData(path)
    }
  }

  test("files without stats are compacted after the files with stats") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createTable(path, Some("d"))
      appendOneFilePerDay(path)
      withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
        spark.range(10)
          .selectExpr("id", "date'2025-01-01' AS d", "named_struct('d', date'2025-01-01') AS n",
            "cast(id AS STRING) AS s")
          .coalesce(1)
          .write.format("delta").mode("append").save(path)
      }
      sql(s"OPTIMIZE delta.`$path`")
      assert(DeltaLog.forTable(spark, path).update().allFiles.count() === 1)
      assert(spark.read.format("delta").load(path).count() === rowsPerDay.sum + 10)
    }
  }

  test("ordering of sort keys") {
    def cmp(k1: Seq[Any], k2: Seq[Any]): Int =
      Integer.signum(StatsBasedFileOrdering.compareKeys(k1.toArray, k2.toArray))
    assert(cmp(Seq(1, 5), Seq(2, 3)) === -1)
    assert(cmp(Seq(1, 5), Seq(1, 3)) === 1)
    assert(cmp(Seq(1, 3), Seq(1, 3)) === 0)
    assert(cmp(Seq(null, 3), Seq(9, 9)) === 1)
    assert(cmp(Seq(9, null), Seq(9, 9)) === 1)
    assert(cmp(Seq("a", "b"), Seq("b", "a")) === -1)
    assert(cmp(Nil, Seq(1, 1)) === 1)
    assert(cmp(Seq(1, 1), Nil) === -1)
    assert(cmp(Nil, Nil) === 0)
  }

  test("binning columns are ignored by ZORDER BY") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createTable(path, Some("d"))
      appendOneFilePerDay(path)
      sql(s"OPTIMIZE delta.`$path` ZORDER BY (id)")
      checkData(path)
    }
  }

  test("invalid binning columns") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(
        s"""CREATE TABLE delta.`$path` (id BIGINT, p INT, d DATE, n STRUCT<d: DATE>, s STRING)
           |USING delta PARTITIONED BY (p)
           |TBLPROPERTIES ('${DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.key}' = '3')
           |""".stripMargin)
      Seq("2026-01-01", "2026-01-02").foreach { day =>
        sql(s"INSERT INTO delta.`$path` VALUES " +
          s"(1, 1, date'$day', named_struct('d', date'$day'), 'a')")
      }

      def checkInvalid(columns: String, expectedMessage: String): Unit = {
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('$binningKey' = '$columns')")
        val e = intercept[AnalysisException] {
          sql(s"OPTIMIZE delta.`$path`")
        }
        assert(e.getMessage.contains(expectedMessage), e.getMessage)
      }

      checkInvalid("missing", "missing")
      checkInvalid("p", "files are already grouped by partition columns")
      checkInvalid("n", "has no min/max statistics")
      // Only the first three data columns (id, d, n.d) have stats.
      checkInvalid("s", "has no min/max statistics")
      val e = intercept[Exception] {
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('$binningKey' = 'd,,s')")
      }
      assert(e.getMessage.contains(binningKey))

      sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('$binningKey' = 'd')")
      sql(s"OPTIMIZE delta.`$path`")
      assert(DeltaLog.forTable(spark, path).update().allFiles.count() === 1)
    }
  }

  test("auto compaction ignores invalid binning columns") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createTable(path, Some("missing"))
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "2") {
        appendOneFilePerDay(path)
      }
      val operations = sql(s"DESCRIBE HISTORY delta.`$path`").select("operation").collect()
      assert(operations.exists(_.getString(0) == "OPTIMIZE"))
      checkData(path)
    }
  }
}
