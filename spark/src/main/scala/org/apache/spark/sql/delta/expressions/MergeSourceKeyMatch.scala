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

package org.apache.spark.sql.delta.expressions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, EmptyBlock, ExprCode, FalseLiteral, TrueLiteral}

/**
 * A MERGE read predicate meaning "the target row's key tuple `targetKeys` equals the key tuple of
 * some row of `sourceKeys`" -- the rows a MERGE whose ON condition has the equi-join conjuncts
 * `targetKeys(i) = source_key_i` can read.
 *
 * Conflict detection evaluates it exactly, as a left-semi join of the concurrently changed rows
 * against `sourceKeys` (see `ConflictDataSkippingReader`); this mirrors DBR's
 * `DeltaTableMergeSourceReadPredicate`, so there is no limit on the number of source keys.
 * Everywhere else (file pruning, partition filters, stats skipping) it evaluates to `true`: it is
 * always recorded next to cheap over-approximating pre-filters on the same keys, which do the
 * pruning, so treating it as `true` there only keeps more rows, never fewer.
 *
 * @param targetKeys the target side of each equi-join conjunct (its only children, so attribute
 *                   rebinding / rewriting applies to them).
 * @param sourceKeys the MERGE source projected to the source side of each conjunct, column `i`
 *                   aligned with `targetKeys(i)`. Driver-only (never shipped to executors).
 */
case class MergeSourceKeyMatch(
    targetKeys: Seq[Expression],
    @transient sourceKeys: DataFrame)
  extends Expression with Predicate {

  override def children: Seq[Expression] = targetKeys

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = true

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    ev.copy(code = EmptyBlock, isNull = FalseLiteral, value = TrueLiteral)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): MergeSourceKeyMatch =
    copy(targetKeys = newChildren)

  override def toString: String = s"merge_source_key_match(${targetKeys.mkString(", ")})"

  override def sql: String = s"merge_source_key_match(${targetKeys.map(_.sql).mkString(", ")})"
}
