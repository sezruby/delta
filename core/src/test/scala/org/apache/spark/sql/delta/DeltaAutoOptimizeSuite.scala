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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.functions._

class DeltaAutoOptimizeSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {
  import testImplicits._

  def writeDataToCheckAutoCompact(
      numFiles: Int,
      dataPath: String,
      partitioned: Boolean = false,
      mode: String = "overwrite"): Unit = {
    val df = spark
      .range(50000)
      .map { _ =>
        (
          scala.util.Random.nextInt(10000000).toLong,
          scala.util.Random.nextInt(1000000000),
          scala.util.Random.nextInt(2))
      }
      .toDF("colA", "colB", "colC")
      .repartition(numFiles)
    if (partitioned) {
      df.write
        .partitionBy("colC")
        .mode(mode)
        .format("delta")
        .save(dataPath)
    } else {
      df.write
        .mode(mode)
        .format("delta")
        .save(dataPath)
    }
  }

  def checkTableVersionAndNumFiles(
      path: String,
      expectedVer: Long,
      expectedNumFiles: Long): Unit = {
    val dt = DeltaLog.forTable(spark, path)
    assert(dt.snapshot.allFiles.count() == expectedNumFiles)
    assert(dt.snapshot.version == expectedVer)
  }

  test("test enabling autoCompact") {
    val tableName = "autoCompactTestTable"
    val tableName2 = s"${tableName}2"
    withTable(tableName, tableName2) {
      withTempDir { dir =>
        val rootPath = dir.getCanonicalPath
        val path = new Path(rootPath, "table1").toString
        var expectedTableVersion = -1
        spark.conf.unset(DeltaSQLConf.AUTO_COMPACT_ENABLED.key)
        writeDataToCheckAutoCompact(100, path)
        // No autoCompact triggered - version should be 0.
        expectedTableVersion += 1
        checkTableVersionAndNumFiles(path, expectedTableVersion, 100)

        // Create table
        spark.sql(s"CREATE TABLE $tableName USING DELTA LOCATION '$path'")
        spark.sql(
          s"ALTER TABLE $tableName SET TBLPROPERTIES (delta.autoOptimize.autoCompact = true)")
        expectedTableVersion += 1 // version increased due to SET TBLPROPERTIES

        writeDataToCheckAutoCompact(100, path)
        expectedTableVersion += 2 // autoCompact should be triggered
        checkTableVersionAndNumFiles(path, expectedTableVersion, 1)

        withSQLConf(DeltaSQLConf.AUTO_COMPACT_ENABLED.key -> "false") {
          // Session config should be prior to table properties
          writeDataToCheckAutoCompact(100, path)
          expectedTableVersion += 1 // autoCompact should not be triggered
          checkTableVersionAndNumFiles(path, expectedTableVersion, 100)
        }

        spark.sql(
          s"ALTER TABLE $tableName SET TBLPROPERTIES (delta.autoOptimize.autoCompact = false)")
        expectedTableVersion += 1 // version increased due to SET TBLPROPERTIES

        withSQLConf(DeltaSQLConf.AUTO_COMPACT_ENABLED.key -> "true") {
          // Session config should be prior to table properties
          writeDataToCheckAutoCompact(100, path)
          expectedTableVersion += 2 // autoCompact should be triggered
          checkTableVersionAndNumFiles(path, expectedTableVersion, 1)
        }

        spark.conf.unset(DeltaSQLConf.AUTO_COMPACT_ENABLED.key)

        // Test default delta table config
        withSQLConf(
          "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact" -> "true") {
          val path2 = new Path(rootPath, "table2").toString
          writeDataToCheckAutoCompact(100, path2)
          // autoCompact should be triggered for path2.
          checkTableVersionAndNumFiles(path2, 1, 1)
        }
      }
    }
  }

  test("test autoCompact configs") {
    val tableName = "autoCompactTestTable"
    withTable(tableName) {
      withTempDir { dir =>
        val rootPath = dir.getCanonicalPath
        val path = new Path(rootPath, "table1").toString
        var expectedTableVersion = -1
        withSQLConf(DeltaSQLConf.AUTO_COMPACT_ENABLED.key -> "true") {
          writeDataToCheckAutoCompact(100, path, partitioned = true)
          expectedTableVersion += 2 // autoCompact should be triggered
          checkTableVersionAndNumFiles(path, expectedTableVersion, 2)

          withSQLConf(DeltaSQLConf.AUTO_COMPACT_MIN_NUM_FILES.key -> "200") {
            writeDataToCheckAutoCompact(100, path, partitioned = true)
            expectedTableVersion += 1 // autoCompact should not be triggered
            checkTableVersionAndNumFiles(path, expectedTableVersion, 200)
          }

          withSQLConf(DeltaSQLConf.AUTO_COMPACT_MAX_FILE_SIZE.key -> "1") {
            writeDataToCheckAutoCompact(100, path, partitioned = true)
            expectedTableVersion += 1 // autoCompact should not be triggered
            checkTableVersionAndNumFiles(path, expectedTableVersion, 200)
          }

          withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> "101024",
            DeltaSQLConf.AUTO_COMPACT_MIN_NUM_FILES.key -> "2") {
            val dt = io.delta.tables.DeltaTable.forPath(path)
            dt.optimize().executeCompaction()
            expectedTableVersion += 1 // autoCompact should not be triggered
            checkTableVersionAndNumFiles(path, expectedTableVersion, 8)
          }

          withSQLConf(DeltaSQLConf.AUTO_COMPACT_MIN_NUM_FILES.key -> "100") {
            writeDataToCheckAutoCompact(100, path, partitioned = true)
            expectedTableVersion += 2 // autoCompact should be triggered
            checkTableVersionAndNumFiles(path, expectedTableVersion, 2)
          }
        }
      }
    }
  }

  test("test max compact data size config") {
    withTempDir { dir =>
      val rootPath = dir.getCanonicalPath
      val path = new Path(rootPath, "table1").toString
      var expectedTableVersion = -1
      writeDataToCheckAutoCompact(100, path, partitioned = true)
      expectedTableVersion += 1
      val dt = io.delta.tables.DeltaTable.forPath(path)
      val dl = DeltaLog.forTable(spark, path)
      val sizeLimit =
        dl.snapshot.allFiles
          .filter(col("path").contains("colC=1"))
          .agg(sum(col("size")))
          .head
          .getLong(0) * 2

      withSQLConf(DeltaSQLConf.AUTO_COMPACT_ENABLED.key -> "true",
        DeltaSQLConf.AUTO_COMPACT_MAX_COMPACT_BYTES.key -> sizeLimit.toString) {
        dt.toDF
          .filter("colC == 1")
          .repartition(50)
          .write
          .format("delta")
          .mode("append")
          .save(path)
        val dl = DeltaLog.forTable(spark, path)
        // version 0: write, 1: append, 2: autoCompact
        assert(dl.snapshot.version == 2)

        {
          val afterAutoCompact = dl.snapshot.allFiles.filter(col("path").contains("colC=1")).count
          val beforeAutoCompact = dl
            .getSnapshotAt(dl.snapshot.version - 1)
            .allFiles
            .filter(col("path").contains("colC=1"))
            .count
          assert(beforeAutoCompact == 150)
          assert(afterAutoCompact == 1)
        }

        {
          val afterAutoCompact = dl.snapshot.allFiles.filter(col("path").contains("colC=0")).count
          val beforeAutoCompact = dl
            .getSnapshotAt(dl.snapshot.version - 1)
            .allFiles
            .filter(col("path").contains("colC=0"))
            .count
          assert(beforeAutoCompact == 100)
          assert(afterAutoCompact == 100)
        }
      }
    }
  }
}
