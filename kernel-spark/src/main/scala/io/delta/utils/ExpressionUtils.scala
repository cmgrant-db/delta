/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.utils

import io.delta.kernel.expressions.{And, Column, Expression}

import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{And => V2And}
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType}

object ExpressionUtils {

  def convertToKernelPredicate(
    expression: V2Expression): Option[io.delta.kernel.expressions.Predicate] = {
    convertToKernelExpression(expression)
      .filter(_.isInstanceOf[io.delta.kernel.expressions.Predicate])
      .asInstanceOf[Option[io.delta.kernel.expressions.Predicate]]
  }

  // TODO extend; consult V2ExpressionBuilder?
  def convertToKernelExpression(expression: V2Expression): Option[Expression] = {
    case e: V2And =>
      (convertToKernelPredicate(e.left()), convertToKernelPredicate(e.right())) match {
        case (Some(left), Some(right)) =>
          Some(new And(left, right))
        case _ => None
      }

    // TODO: equals
    // case e: Predicate if e.name == "=" =>

    case c: NamedReference =>
      new Column(c.fieldNames)
    // TODO do we need to check that the column is valid?

    case l: Literal[Boolean] =>
      assert(l.dataType.isInstanceOf[BooleanType])
      Some(io.delta.kernel.expressions.Literal.ofBoolean(l.value))
    case l: Literal[Int] =>
      assert(l.dataType.isInstanceOf[IntegerType])
      Some(io.delta.kernel.expressions.Literal.ofInt(l.value))
    case l: Literal[Long] =>
      assert(l.dataType.isInstanceOf[LongType])
      Some(io.delta.kernel.expressions.Literal.ofLong(l.value))
    case l: Literal[String] =>
      assert(l.dataType.isInstanceOf[StringType])
      Some(io.delta.kernel.expressions.Literal.ofString(l.value))
  }
}
