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

package org.apache.spark.sql.delta.skipping.clustering

import org.apache.spark.sql.delta.skipping.ClusteredTableTestUtils
import org.apache.spark.sql.delta.skipping.clustering.temp.{AlterTableReplacePartitionedByWithClusterBy, ClusterBySpec}
import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaLog}
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.delta.commands.ReplacePartitionedByUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DataSkippingDeltaTestsUtils
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedTable
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, TimestampType}

class ReplacePartitionedByWithClusterBySuite
  extends QueryTest
  with ClusteredTableTestUtils
  with DeltaSQLCommandTest
  with DataSkippingDeltaTestsUtils {

  private val opName = "REPLACE PARTITIONED BY WITH CLUSTER BY"

  private def createPartitionedTable(path: String, partitionColumns: String*): Unit = {
    spark.range(0, 100, 1, 4)
      .selectExpr("id", "cast(id % 5 as string) as p", "cast(id % 3 as int) as q", "id * 2 as v")
      .write.format("delta").partitionBy(partitionColumns: _*).save(path)
  }

  private def readTable(path: String) = spark.read.format("delta").load(path)

  private def statsOf(file: AddFile): Map[String, Any] =
    JsonUtils.fromJson[Map[String, Any]](file.stats)

  test("parser") {
    val parser = spark.sessionState.sqlParser
    val none = parser.parsePlan("ALTER TABLE t REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
    assert(none === AlterTableReplacePartitionedByWithClusterBy(
      UnresolvedTable(Seq("t"), "ALTER TABLE ... REPLACE PARTITIONED BY WITH CLUSTER BY"), None))
    val cols = parser.parsePlan(
      "ALTER TABLE a.t REPLACE PARTITIONED BY WITH CLUSTER BY (c1, s.c2)")
    assert(cols === AlterTableReplacePartitionedByWithClusterBy(
      UnresolvedTable(Seq("a", "t"), "ALTER TABLE ... REPLACE PARTITIONED BY WITH CLUSTER BY"),
      Some(ClusterBySpec(Seq(Seq("c1"), Seq("s", "c2"))))))
    // `with` is not reserved.
    parser.parsePlan("VACUUM with")
  }

  test("CLUSTER BY NONE drops the partitioning without rewriting data files") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedTable(path, "p", "q")
      val deltaLog = DeltaLog.forTable(spark, path)
      val before = deltaLog.update()
      val expected = readTable(path).collect()

      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")

      val after = deltaLog.update()
      assert(after.version === before.version + 1)
      assert(after.metadata.partitionColumns.isEmpty)
      assert(after.schema === before.schema)
      // No new table features, the table is a plain unpartitioned table.
      assert(after.protocol === before.protocol)
      assert(!ClusteredTableUtils.isSupported(after.protocol))
      assert(after.allFiles.collect().map(_.path).toSet ===
        before.allFiles.collect().map(_.path).toSet)
      assert(after.allFiles.collect().forall(_.partitionValues.isEmpty))
      checkAnswer(readTable(path), expected)

      val lastCommit = deltaLog.history.getHistory(Some(1)).head
      assert(lastCommit.operation === opName)
      assert(lastCommit.operationParameters("oldPartitioningColumns") === "p,q")
      assert(lastCommit.operationParameters("newClusteringColumns") === "")

      val changes = deltaLog.getChanges(after.version).next()._2
      assert(changes.collect { case a: AddFile => a }.forall(!_.dataChange))
      assert(!changes.exists(_.isInstanceOf[RemoveFile]))
      assert(lastCommit.operationMetrics.flatMap(_.get("numRewrittenFiles")) === Some("0"))

      // Time travel to the partitioned version still works.
      checkAnswer(
        spark.read.format("delta").option("versionAsOf", before.version).load(path), expected)
    }
  }

  test("statistics are synthesized for the former partition columns") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedTable(path, "p", "q")
      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.update().allFiles.collect().foreach { f =>
        val stats = statsOf(f)
        val p = stats("minValues").asInstanceOf[Map[String, Any]]("p")
        assert(stats("maxValues").asInstanceOf[Map[String, Any]]("p") === p)
        assert(stats("nullCount").asInstanceOf[Map[String, Any]]("p") === 0)
        assert(stats("minValues").asInstanceOf[Map[String, Any]].contains("q"))
        assert(stats("minValues").asInstanceOf[Map[String, Any]].contains("id"))
      }
      // Each former partition value is in its own set of files, which can be skipped.
      val numFiles = deltaLog.update().numOfFiles
      val numFilesForP3 = filesRead(spark, deltaLog, "p = '3'", checkEmptyUnusedFilters = false)
      assert(numFilesForP3 > 0 && numFilesForP3 < numFiles)
      checkAnswer(
        readTable(path).where("p = '3'").selectExpr("count(*)"), Row(20L))
      val numFilesForQ1 = filesRead(spark, deltaLog, "q = 1", checkEmptyUnusedFilters = false)
      assert(numFilesForQ1 > 0 && numFilesForQ1 < numFiles)
    }
  }

  test("writes, DML and OPTIMIZE after dropping the partitioning") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedTable(path, "p")
      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
      val deltaLog = DeltaLog.forTable(spark, path)
      val filesBefore = deltaLog.update().allFiles.collect().map(_.path).toSet

      spark.range(100, 110).selectExpr("id", "'9' as p", "cast(id % 3 as int) as q", "id * 2 as v")
        .write.format("delta").mode("append").save(path)
      val newFiles = deltaLog.update().allFiles.collect().filterNot(f => filesBefore(f.path))
      assert(newFiles.nonEmpty)
      // New files are written at the root of the table.
      assert(newFiles.forall(f => !f.path.contains("/") && f.partitionValues.isEmpty))

      sql(s"UPDATE delta.`$path` SET v = -1 WHERE p = '1'")
      sql(s"DELETE FROM delta.`$path` WHERE p = '2'")
      sql(s"""MERGE INTO delta.`$path` t USING (SELECT 0L AS id, 'x' AS p) s ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET t.p = s.p""".stripMargin)
      checkAnswer(readTable(path).where("p = '1'").selectExpr("count(*)", "max(v)"),
        Row(20L, -1L))
      checkAnswer(readTable(path).where("p = '2'").selectExpr("count(*)"), Row(0L))
      checkAnswer(readTable(path).where("id = 0").select("p"), Row("x"))
      checkAnswer(readTable(path).selectExpr("count(*)"), Row(90L))

      sql(s"OPTIMIZE delta.`$path`")
      checkAnswer(readTable(path).selectExpr("count(*)"), Row(90L))
      assert(deltaLog.update().metadata.partitionColumns.isEmpty)
    }
  }

  test("CLUSTER BY columns converts a partitioned table into a clustered table") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedTable(path, "p")
      val expected = readTable(path).collect()
      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY (p, id)")
      val deltaLog = DeltaLog.forTable(spark, path)
      val snapshot = deltaLog.update()
      assert(snapshot.metadata.partitionColumns.isEmpty)
      assert(ClusteredTableUtils.isSupported(snapshot.protocol))
      verifyClusteringColumnsInDomainMetadata(snapshot, Seq("p", "id"))
      checkAnswer(readTable(path), expected)
      val lastCommit = deltaLog.history.getHistory(Some(1)).head
      assert(lastCommit.operation === opName)
      assert(lastCommit.operationParameters("newClusteringColumns") === "p,id")

      // The table can be clustered and further altered like any clustered table.
      sql(s"OPTIMIZE delta.`$path`")
      checkAnswer(readTable(path), expected)
      sql(s"ALTER TABLE delta.`$path` CLUSTER BY (id)")
      verifyClusteringColumnsInDomainMetadata(deltaLog.update(), Seq("id"))
    }
  }

  test("works with catalog tables") {
    withTable("tbl") {
      sql("CREATE TABLE tbl (id BIGINT, p STRING) USING delta PARTITIONED BY (p)")
      sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, null)")
      sql("ALTER TABLE tbl REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
      checkAnswer(sql("SELECT * FROM tbl"), Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, null)))
      checkAnswer(sql("SELECT id FROM tbl WHERE p IS NULL"), Row(3L))
      val detail = sql("DESCRIBE DETAIL tbl").select("partitionColumns").head()
      assert(detail.getSeq[String](0).isEmpty)
    }
  }

  test("works with column mapping") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(s"""CREATE TABLE delta.`$path` (id BIGINT, p STRING) USING delta PARTITIONED BY (p)
             |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
      sql(s"INSERT INTO delta.`$path` VALUES (1, 'a'), (2, 'b')")
      sql(s"ALTER TABLE delta.`$path` RENAME COLUMN p TO p2")
      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
      checkAnswer(readTable(path), Seq(Row(1L, "a"), Row(2L, "b")))
      checkAnswer(readTable(path).where("p2 = 'b'").select("id"), Row(2L))
      val deltaLog = DeltaLog.forTable(spark, path)
      assert(filesRead(spark, deltaLog, "p2 = 'b'", checkEmptyUnusedFilters = false) === 1)
    }
  }

  test("works with deletion vectors and row tracking") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withSQLConf(
          "spark.databricks.delta.properties.defaults.enableDeletionVectors" -> "true",
          "spark.databricks.delta.properties.defaults.enableRowTracking" -> "true") {
        createPartitionedTable(path, "p")
      }
      sql(s"DELETE FROM delta.`$path` WHERE id % 10 = 0")
      val deltaLog = DeltaLog.forTable(spark, path)
      val before = deltaLog.update()
      assert(before.allFiles.collect().exists(_.deletionVector != null))
      val expected = readTable(path).selectExpr("*", "_metadata.row_id").collect()

      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")

      val after = deltaLog.update()
      val beforeByPath = before.allFiles.collect().map(f => f.path -> f).toMap
      after.allFiles.collect().foreach { f =>
        val old = beforeByPath(f.path)
        assert(f.deletionVector === old.deletionVector)
        assert(f.baseRowId === old.baseRowId)
        assert(f.defaultRowCommitVersion === old.defaultRowCommitVersion)
      }
      checkAnswer(readTable(path).selectExpr("*", "_metadata.row_id"), expected)
    }
  }

  test("fails on an unpartitioned table") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedTable(path)
      val e = intercept[DeltaAnalysisException] {
        sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
      }
      assert(e.getErrorClass === "DELTA_REPLACE_PARTITIONED_BY_ON_UNPARTITIONED_TABLE")
    }
  }

  private def filesWithoutPartitionColumns(path: String, partitionColumns: String*) = {
    val deltaLog = DeltaLog.forTable(spark, path)
    ReplacePartitionedByUtils.findFilesWithoutPartitionColumns(
      spark, deltaLog, deltaLog.update(), partitionColumns)
  }

  private def lastOperationMetrics(path: String): Map[String, String] =
    sql(s"DESCRIBE HISTORY delta.`$path` LIMIT 1")
      .select("operationMetrics").head().getMap[String, String](0).toMap

  test("rewrites the data files that do not store the partition columns") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(s"""CREATE TABLE delta.`$path` (id BIGINT, p STRING) USING delta PARTITIONED BY (p)
             |TBLPROPERTIES ('delta.writePartitionColumnsToParquet' = 'false')""".stripMargin)
      sql(s"INSERT INTO delta.`$path` VALUES (1, 'a'), (2, 'b')")
      sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
        "('delta.writePartitionColumnsToParquet' = 'true')")
      sql(s"INSERT INTO delta.`$path` VALUES (3, 'a'), (4, null)")
      val deltaLog = DeltaLog.forTable(spark, path)
      val before = deltaLog.update()
      val notMaterialized = filesWithoutPartitionColumns(path, "p").map(_.path).toSet
      assert(notMaterialized.size === 2)
      val materialized = before.allFiles.collect().map(_.path).toSet -- notMaterialized
      assert(materialized.size === 2)

      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")

      val after = deltaLog.update()
      assert(after.version === before.version + 1)
      assert(after.metadata.partitionColumns.isEmpty)
      val afterFiles = after.allFiles.collect()
      assert(afterFiles.forall(_.partitionValues.isEmpty))
      val afterPaths = afterFiles.map(_.path).toSet
      assert(materialized.subsetOf(afterPaths))
      assert((afterPaths intersect notMaterialized).isEmpty)
      assert(filesWithoutPartitionColumns(path, "p").isEmpty)

      val changes = deltaLog.getChanges(after.version).next()._2
      val removed = changes.collect { case r: RemoveFile => r }
      assert(removed.map(_.path).toSet === notMaterialized)
      assert(removed.forall(!_.dataChange))
      assert(changes.collect { case a: AddFile => a }.forall(!_.dataChange))
      val metrics = lastOperationMetrics(path)
      assert(metrics("numRewrittenFiles") === "2")
      assert(metrics("numAddedFilesFromRewrite").toInt > 0)

      checkAnswer(readTable(path),
        Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "a"), Row(4L, null)))
      checkAnswer(readTable(path).where("p = 'b'").select("id"), Row(2L))
      checkAnswer(readTable(path).where("p IS NULL").select("id"), Row(4L))
    }
  }

  test("rewrite applies deletion vectors and preserves row tracking") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(s"""CREATE TABLE delta.`$path` (id BIGINT, p STRING) USING delta PARTITIONED BY (p)
             |TBLPROPERTIES (
             |  'delta.writePartitionColumnsToParquet' = 'false',
             |  'delta.enableDeletionVectors' = 'true',
             |  'delta.enableRowTracking' = 'true')""".stripMargin)
      spark.range(0, 100, 1, 4).selectExpr("id", "cast(id % 3 as string) as p")
        .write.format("delta").mode("append").save(path)
      sql(s"DELETE FROM delta.`$path` WHERE id % 10 = 0")
      val deltaLog = DeltaLog.forTable(spark, path)
      assert(deltaLog.update().allFiles.collect().exists(_.deletionVector != null))
      val rowTrackingColumns =
        Seq("*", "_metadata.row_id", "_metadata.row_commit_version")
      val expected = readTable(path).selectExpr(rowTrackingColumns: _*).collect()
      assert(expected.length === 90)

      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")

      val after = deltaLog.update()
      assert(after.metadata.partitionColumns.isEmpty)
      assert(after.allFiles.collect().forall(_.deletionVector == null))
      assert(filesWithoutPartitionColumns(path, "p").isEmpty)
      checkAnswer(readTable(path).selectExpr(rowTrackingColumns: _*), expected)
    }
  }

  test("fails when data files do not store the partition columns and rewrite is disabled") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(s"""CREATE TABLE delta.`$path` (id BIGINT, p STRING) USING delta PARTITIONED BY (p)
             |TBLPROPERTIES ('delta.writePartitionColumnsToParquet' = 'false')""".stripMargin)
      sql(s"INSERT INTO delta.`$path` VALUES (1, 'a'), (2, 'b')")
      val deltaLog = DeltaLog.forTable(spark, path)
      val versionBefore = deltaLog.update().version
      val e = withSQLConf(DeltaSQLConf
          .DELTA_REPLACE_PARTITIONED_BY_REWRITE_NON_MATERIALIZED_FILES.key -> "false") {
        intercept[DeltaAnalysisException] {
          sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
        }
      }
      assert(e.getErrorClass ===
        "DELTA_REPLACE_PARTITIONED_BY_PARTITION_COLUMNS_NOT_MATERIALIZED")
      assert(e.getMessage.contains("2 data file(s)"))
      assert(e.getMessage.contains(
        DeltaSQLConf.DELTA_REPLACE_PARTITIONED_BY_REWRITE_NON_MATERIALIZED_FILES.key))
      assert(deltaLog.update().version === versionBefore)
      assert(deltaLog.update().metadata.partitionColumns === Seq("p"))
    }
  }

  test("verification can be disabled") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedTable(path, "p")
      withSQLConf(DeltaSQLConf
          .DELTA_REPLACE_PARTITIONED_BY_VERIFY_MATERIALIZED_PARTITION_COLUMNS.key -> "false") {
        sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
      }
      checkAnswer(readTable(path).selectExpr("count(*)"), Row(100L))
    }
  }

  test("fails on a non-existing clustering column") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedTable(path, "p")
      intercept[Exception] {
        sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY (missing)")
      }
      assert(DeltaLog.forTable(spark, path).update().metadata.partitionColumns === Seq("p"))
    }
  }

  test("addPartitionValuesToStats") {
    val fields = Seq(
      StructField("s", StringType), StructField("i", IntegerType),
      StructField("d", DateType), StructField("t", TimestampType), StructField("n", StringType))
    val stats =
      """{"numRecords":3,"minValues":{"id":1},"maxValues":{"id":3},"nullCount":{"id":0}}"""
    val values = Map("s" -> "abc", "i" -> "7", "d" -> "2024-01-02",
      "t" -> "2024-01-02 03:04:05", "n" -> null)
    val result = JsonUtils.fromJson[Map[String, Map[String, Any]]](
      ReplacePartitionedByUtils.addPartitionValuesToStats(stats, values, fields, 32)
        .replace("\"numRecords\":3,", ""))
    assert(result("minValues") === Map("id" -> 1, "s" -> "abc", "i" -> 7, "d" -> "2024-01-02"))
    assert(result("maxValues") === Map("id" -> 3, "s" -> "abc", "i" -> 7, "d" -> "2024-01-02"))
    assert(result("nullCount") ===
      Map("id" -> 0, "s" -> 0, "i" -> 0, "d" -> 0, "t" -> 0, "n" -> 3))
    // Long strings are not added since collected string stats would be truncated.
    assert(!ReplacePartitionedByUtils.addPartitionValuesToStats(
      stats, Map("s" -> ("x" * 33)), fields.take(1), 32).contains("xxx"))
    // Files without stats are left untouched.
    assert(ReplacePartitionedByUtils.addPartitionValuesToStats(null, values, fields, 32) === null)
    assert(ReplacePartitionedByUtils.addPartitionValuesToStats("", values, fields, 32) === "")
  }
}
