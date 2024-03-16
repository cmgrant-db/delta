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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import io.delta.golden.GoldenTableUtils.goldenTablePath

class DeltaSparkSuite extends QueryTest with SharedSparkSession {

  test("trial") {
    spark.read.format("delta").load(goldenTablePath("dv-partitioned-with-checkpoint")).show()
  }

  // TODO add some tests, DVs work, column mapping works, partition columns, checkpoints,
  //  partition pruning & data skipping, etc

  // todo: better way to check the number of read files?

  test("filter pushdown: partition predicate") {
    // should only spawn 2 tasks check the logs
    spark.read.format("delta").load(goldenTablePath("dv-partitioned-with-checkpoint"))
      .where("part = 5")
      .show()
  }

  test("filter pushdown: data skipping predicate") {
    // should spawn 0 tasks
    spark.read.format("delta")
      .load(goldenTablePath("dv-with-columnmapping"))
      .where("col1 = 50")
      .show()
  }

  test("check schema pruning works") {
    // check logs for "INFO DefaultParquetHandler" to see the parquet schema read which does not
    // include col2
    spark.read.format("delta").load(goldenTablePath("dv-partitioned-with-checkpoint"))
      .select("col1", "part")
      .show()
  }
}
