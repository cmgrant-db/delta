/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.utils

import io.delta.kernel.expressions.{Column, Expression, Literal, Predicate}

/** Useful helper functions for creating expressions in tests */
trait ExpressionUtils {

  def equals(e1: Expression, e2: Expression): Predicate = {
    new Predicate("=", e1, e2)
  }

  def lessThan(e1: Expression, e2: Expression): Predicate = {
    new Predicate("<", e1, e2)
  }

  def greaterThan(e1: Expression, e2: Expression): Predicate = {
    new Predicate(">", e1, e2)
  }

  def greaterThanOrEqual(e1: Expression, e2: Expression): Predicate = {
    new Predicate(">=", e1, e2)
  }

  def lessThanOrEqual(e1: Expression, e2: Expression): Predicate = {
    new Predicate("<=", e1, e2)
  }

  def not(pred: Predicate): Predicate = {
    new Predicate("NOT", pred)
  }

  def col(name: String): Column = new Column(name)

  def nestedCol(name: String): Column = {
    new Column(name.split("\\."))
  }

  /* ---------- NOT-YET SUPPORTED EXPRESSIONS ----------- */

  def nullSafeEquals(e1: Expression, e2: Expression): Predicate = {
    new Predicate("<=>", e1, e2)
  }

  def notEquals(e1: Expression, e2: Expression): Predicate = {
    new Predicate("<>", e1, e2)
  }

  def startsWith(e1: Expression, e2: Expression): Predicate = {
    new Predicate("STARTS_WITH", e1, e2)
  }
}
