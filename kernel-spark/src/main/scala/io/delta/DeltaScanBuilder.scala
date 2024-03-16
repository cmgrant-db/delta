/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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
package io.delta

import io.delta.kernel.client.TableClient
import io.delta.kernel.expressions.And
import io.delta.utils.{ExpressionUtils, SchemaUtils}

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType

class DeltaScanBuilder(
  tableClient: TableClient,
  snapshot: io.delta.kernel.Snapshot) extends ScanBuilder
  with SupportsPushDownRequiredColumns
  with SupportsPushDownV2Filters {

  private var kernelScanBuilder = snapshot.getScanBuilder(tableClient)
  private var readSchema: StructType = SchemaUtils.convertToSparkSchema(
    snapshot.getSchema(tableClient))
  private var pushedFilters: Array[Predicate] = Array.empty

  // TODO: do we take the below into account?
  /**
   * Applies column pruning w.r.t. the given requiredSchema.
   * <p>
   * Implementation should try its best to prune the unnecessary columns or nested fields, but it's
   * also OK to do the pruning partially, e.g., a data source may not be able to prune nested
   * fields, and only prune top-level columns.
   * <p>
   * Note that, {@link Scan# readSchema ( )} implementation should take care of the column
   * pruning applied here.
   */
  override def pruneColumns(requiredSchema: StructType): Unit = {
    readSchema = requiredSchema
    kernelScanBuilder = kernelScanBuilder.withReadSchema(
      tableClient,
      SchemaUtils.convertFromSparkSchema(requiredSchema))
  }

  /**
   * Pushes down predicates, and returns predicates that need to be evaluated after scanning.
   * <p>
   * Rows should be returned from the data source if and only if all of the predicates match.
   * That is, predicates must be interpreted as ANDed together.
   */
  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    // TODO this should return the remaining filter. This means we would need to fix the Scan here
    //   or find some other way to get the remaining filter this early... for now return all.

    val predToKernelExpr = predicates.map(p => p -> ExpressionUtils.convertToKernelPredicate(p))
    pushedFilters = predToKernelExpr.filter(_._2.nonEmpty).map(_._1)
    val filterToPush = predToKernelExpr.flatMap(_._2).reduceOption((l, r) => new And(l, r))
    if (filterToPush.nonEmpty) {
      kernelScanBuilder = kernelScanBuilder.withFilter(tableClient, filterToPush.get)
      // scalastyle:off
      println(s"filter pushed = ${filterToPush.get}")
      // scalastyle:on
    }
    predicates // for now since we don't know which are partition preds we return all
  }

  /**
   * Returns the predicates that are pushed to the data source via
   * {@link # pushPredicates ( Predicate [ ] )}.
   * <p>
   * There are 3 kinds of predicates:
   * <ol>
   * <li>pushable predicates which don't need to be evaluated again after scanning.</li>
   * <li>pushable predicates which still need to be evaluated after scanning, e.g. parquet row
   * group predicate.</li>
   * <li>non-pushable predicates.</li>
   * </ol>
   * <p>
   * Both case 1 and 2 should be considered as pushed predicates and should be returned
   * by this method.
   * <p>
   * It's possible that there is no predicates in the query and
   * {@link # pushPredicates ( Predicate [ ] )} is never called,
   * empty array should be returned for this case.
   */
  override def pushedPredicates(): Array[Predicate] = {
    pushedFilters
  }

  def build(): Scan = {
    new DeltaScan(tableClient, kernelScanBuilder.build(), readSchema)
  }
}
