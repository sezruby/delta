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

import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, GreaterThanOrEqual, LessThan, Literal, Remainder}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

/**
 * Tests for the delete/read row-level refinement of conflict detection (row-level concurrency
 * Case 1b, [[DeltaSQLConf.DELTA_CONFLICT_DETECTION_DELETE_READ_DATA_SKIPPING_ENABLED]]).
 *
 * The path-keyed delete/read check aborts the current transaction whenever a file the winner
 * removed is in the current transaction's read set, regardless of whether the removed rows actually
 * match what it read. This refinement reads the rows the winner ACTUALLY removed and conflicts only
 * when one matches the read predicate. It is the delete/read analogue of the value-exact
 * added-files skipping in [[ConflictDataSkippingSuite]]; it must be one-way safe (abort unless the
 * removed rows are proven not to match) and fail-safe (any error keeps today's abort).
 */
class DeleteReadConflictDataSkippingSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  private def tableRef(dir: File): String = s"delta.`${dir.getCanonicalPath}`"

  private val id: AttributeReference = AttributeReference("id", LongType)()
  private def lt(v: Long): Expression = LessThan(id, Literal(v))
  private def ge(v: Long): Expression = GreaterThanOrEqual(id, Literal(v))
  private def even: Expression = EqualTo(Remainder(id, Literal(2L)), Literal(0L))
  private def odd: Expression = EqualTo(Remainder(id, Literal(2L)), Literal(1L))

  private def manufacturedAdd(name: String): AddFile =
    AddFile(name, Map.empty[String, String], size = 1L, modificationTime = 1L, dataChange = true)

  // ---------------------------------------------------------------------------------------------
  // Direct reader tests: the count-difference core, exercised against real deletion vectors.
  // ---------------------------------------------------------------------------------------------

  test("anyRemovedRowMatchesReadPredicate: true only when a removed row matches (real DVs)") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      // DV-enabled single file, ids [0, 100).
      spark.range(0, 100).repartition(1)
        .write.format("delta").mode("append").save(path)
      sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
        s"('delta.enableDeletionVectors' = 'true')")
      val log = DeltaLog.forTable(spark, path)

      // Pre-image: the file with no deletions (all of [0, 100) live).
      val preImage = log.update().allFiles.collect().toSeq
      assert(preImage.size == 1, s"expected a single file, got ${preImage.size}")

      // Winner deletes ids [0, 10) via a merge-on-read deletion vector (same file re-added).
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id < 10")
      val postSnapshot = log.update()
      val postImage = postSnapshot.allFiles.collect().toSeq
      assert(postImage.size == 1, "expected the same file re-added with a DV, not a COW rewrite")
      assert(postImage.head.path == preImage.head.path, "a DV delete must re-add at the same path")
      assert(postImage.head.deletionVector != null, "expected a deletion vector (merge-on-read)")

      def matches(pred: Expression): Boolean =
        postSnapshot.anyRemovedRowMatchesReadPredicate(preImage, postImage, Seq(Seq(pred)))

      // Removed rows are exactly [0, 10).
      assert(matches(lt(5)), "id < 5 intersects the removed rows [0, 10) -> conflict")
      assert(matches(lt(10)), "id < 10 equals the removed rows -> conflict")
      assert(!matches(ge(50)), "id >= 50 is disjoint from the removed rows -> no conflict")
      assert(!matches(ge(10)), "id >= 10 excludes the removed rows -> no conflict")
    }
  }

  test("anyRemovedRowMatchesReadPredicate: full-file removal counts every pre-image row removed") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 100).repartition(1)
        .write.format("delta").mode("append").save(path)
      val log = DeltaLog.forTable(spark, path)
      val preImage = log.update().allFiles.collect().toSeq
      assert(preImage.size == 1)
      val snapshot = log.update()

      // No post-image file (a full-file removal): matches(removed) == matches(pre-image live rows).
      def matches(pred: Expression): Boolean =
        snapshot.anyRemovedRowMatchesReadPredicate(preImage, Seq.empty, Seq(Seq(pred)))

      assert(matches(lt(5)), "some removed row satisfies id < 5 -> conflict")
      assert(!matches(ge(200)), "no removed row satisfies id >= 200 -> no conflict")
    }
  }

  test("anyRemovedRowMatchesReadPredicate: OR across reads, conflict if any read matches") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 100).repartition(1)
        .write.format("delta").mode("append").save(path)
      val log = DeltaLog.forTable(spark, path)
      val preImage = log.update().allFiles.collect().toSeq
      val snapshot = log.update()

      // Removed = whole pre-image. One read disjoint (id >= 200), one matching (id < 5) => conflict
      assert(
        snapshot.anyRemovedRowMatchesReadPredicate(
          preImage, Seq.empty, Seq(Seq(ge(200)), Seq(lt(5)))),
        "a removed row matches the second read -> conflict")
      // Both reads disjoint from [0, 100) -> no conflict.
      assert(
        !snapshot.anyRemovedRowMatchesReadPredicate(
          preImage, Seq.empty, Seq(Seq(ge(200)), Seq(lt(-5)))),
        "no removed row matches either read -> no conflict")
    }
  }

  test("anyRemovedRowMatchesReadPredicate: a read with no eligible filter conflicts") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 100).repartition(1)
        .write.format("delta").mode("append").save(path)
      val log = DeltaLog.forTable(spark, path)
      val preImage = log.update().allFiles.collect().toSeq
      val snapshot = log.update()

      // An empty read matches everything, so we cannot prove non-match -> conservative conflict.
      assert(snapshot.anyRemovedRowMatchesReadPredicate(preImage, Seq.empty, Seq(Seq.empty)),
        "a read with no eligible filter must conflict")
    }
  }

  test("anyRemovedRowMatchesReadPredicate: unresolvable predicate falls back to conflict") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 100).repartition(1)
        .write.format("delta").mode("append").save(path)
      val log = DeltaLog.forTable(spark, path)
      val preImage = log.update().allFiles.collect().toSeq
      val snapshot = log.update()

      // A predicate on a column that does not exist: rebinding throws and the fail-safe conflicts
      // rather than letting the scan failure silently pass (or abort) the commit.
      val ghost = EqualTo(AttributeReference("does_not_exist", LongType)(), Literal(1L))
      assert(snapshot.anyRemovedRowMatchesReadPredicate(preImage, Seq.empty, Seq(Seq(ghost))),
        "an unresolvable predicate must fall back to conflict")
    }
  }

  // ---------------------------------------------------------------------------------------------
  // End-to-end tests: a reader loser racing a concurrent DELETE winner. The winner removes an
  // entire file (all rows match), so it re-adds nothing -- only the delete/read arm is exercised,
  // never the added-files arm.
  // ---------------------------------------------------------------------------------------------

  /**
   * Runs a reader loser that scans the single all-even file under `readPredicate` against a winner
   * that deletes the whole file, with the refinement toggled by `enabled`. Returns whether the
   * loser committed (true) or aborted with a delete/read conflict (false).
   */
  private def runDeleteReadRace(readPredicate: Expression, enabled: Boolean): Boolean = {
    var committed = false
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      // Single file of all-EVEN ids in [0, 100). DVs are OFF (the default), so the winner's
      // whole-file delete removes it outright with no re-added survivor file.
      spark.range(0, 100, step = 2).repartition(1)
        .write.format("delta").mode("append").save(path)
      val log = DeltaLog.forTable(spark, path)
      withSQLConf(
          DeltaSQLConf.DELTA_CONFLICT_DETECTION_DELETE_READ_DATA_SKIPPING_ENABLED.key ->
            enabled.toString) {
        val loser = log.startTransaction()
        val readFiles = loser.filterFiles(Seq(readPredicate))
        assert(readFiles.size == 1, s"loser must read the file, got ${readFiles.size}")

        // Winner removes the whole file (every row matches) -> RemoveFile only, no AddFile.
        sql(s"DELETE FROM ${tableRef(dir)} WHERE id >= 0")

        committed =
          try {
            loser.commit(
              Seq(manufacturedAdd("loser.parquet")), DeltaOperations.Write(SaveMode.Append))
            true
          } catch {
            case _: io.delta.exceptions.ConcurrentDeleteReadException => false
          }
      }
    }
    committed
  }

  test("e2e: removed rows disjoint from the read predicate -> loser commits when enabled") {
    // The winner removed all-even rows; the loser only read odd rows, so no removed row matches.
    assert(runDeleteReadRace(odd, enabled = true),
      "removed rows are all even, read predicate is odd -> no conflict")
  }

  test("e2e: removed rows match the read predicate -> loser still conflicts when enabled") {
    // The winner removed all-even rows; the loser read even rows, so removed rows match.
    assert(!runDeleteReadRace(even, enabled = true),
      "removed rows are even, read predicate is even -> conflict")
  }

  test("e2e: feature disabled -> disjoint removed rows still conflict (path-keyed)") {
    assert(!runDeleteReadRace(odd, enabled = false),
      "with the refinement off, any removed-and-read file conflicts")
  }

  test("e2e: whole-table read is never refined -> conflicts with a concurrent delete") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 100, step = 2).repartition(1)
        .write.format("delta").mode("append").save(path)
      val log = DeltaLog.forTable(spark, path)
      withSQLConf(
          DeltaSQLConf.DELTA_CONFLICT_DETECTION_DELETE_READ_DATA_SKIPPING_ENABLED.key -> "true") {
        val loser = log.startTransaction()
        loser.readWholeTable()
        sql(s"DELETE FROM ${tableRef(dir)} WHERE id >= 0")
        intercept[io.delta.exceptions.ConcurrentDeleteReadException] {
          loser.commit(
            Seq(manufacturedAdd("loser.parquet")), DeltaOperations.Write(SaveMode.Append))
        }
      }
    }
  }
}
