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
package io.delta.kernel.defaults

import scala.collection.JavaConverters._

import io.delta.golden.GoldenTableUtils.goldenTablePath
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import io.delta.kernel.client.{FileReadContext, JsonHandler, ParquetHandler, TableClient}
import io.delta.kernel.data.{FileDataReadResult, FilteredColumnarBatch}
import io.delta.kernel.expressions.{AlwaysFalse, AlwaysTrue, And, Column, Literal, Predicate, ScalarExpression}
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterator
import io.delta.kernel.{Snapshot, Table}
import io.delta.kernel.defaults.client.{DefaultJsonHandler, DefaultParquetHandler, DefaultTableClient}
import io.delta.kernel.defaults.utils.TestUtils

class ScanSuite extends AnyFunSuite with TestUtils {
  import io.delta.kernel.defaults.ScanSuite._

  def checkSkipping(tablePath: String, hits: Seq[Predicate], misses: Seq[Predicate]): Unit = {
    val snapshot = latestSnapshot(tablePath)
    hits.foreach { predicate =>
      val scanFiles = collectScanFileRows(
        snapshot.getScanBuilder(defaultTableClient)
          .withFilter(defaultTableClient, predicate)
          .build())
      assert(scanFiles.nonEmpty)
    }
    misses.foreach { predicate =>
      val scanFiles = collectScanFileRows(
        snapshot.getScanBuilder(defaultTableClient)
          .withFilter(defaultTableClient, predicate)
          .build())
      assert(scanFiles.isEmpty)
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Skipping tests from Spark's DataSkippingDeltaTests
  //////////////////////////////////////////////////////////////////////////////////

  test("data skipping - top level, single 1") {
    checkSkipping(
      goldenTablePath("data-skipping-top-level-single-1"),
      hits = Seq(
        AlwaysTrue.ALWAYS_TRUE, // trivial base case
        new Predicate("=", new Column("a"), Literal.ofInt(1)),
        new Predicate("=", Literal.ofInt(1), new Column("a")),
        new Predicate(">=", new Column("a"), Literal.ofInt(1)),
        new Predicate("<=", new Column("a"), Literal.ofInt(1)),
        new Predicate("<=", new Column("a"), Literal.ofInt(2)),
        new Predicate(">=", new Column("a"), Literal.ofInt(0)),
        new Predicate("<=", Literal.ofInt(1), new Column("a")),
        new Predicate(">=", Literal.ofInt(1), new Column("a")),
        new Predicate(">=", Literal.ofInt(2), new Column("a")),
        new Predicate("<=", Literal.ofInt(0), new Column("a"))
        /*
        "a <=> 1",
        "1 <=> a",
        "NOT a <=> 2"
         */
      ),
      misses = Seq(
        new Predicate("=", new Column("a"), Literal.ofInt(2)),
        new Predicate("=", Literal.ofInt(2), new Column("a")),
        new Predicate(">", new Column("a"), Literal.ofInt(1)),
        new Predicate("<", new Column("a"), Literal.ofInt(1)),
        new Predicate(">=", new Column("a"), Literal.ofInt(2)),
        new Predicate("<=", new Column("a"), Literal.ofInt(0)),
        new Predicate("<", Literal.ofInt(1), new Column("a")),
        new Predicate(">", Literal.ofInt(1), new Column("a")),
        new Predicate("<=", Literal.ofInt(2), new Column("a")),
        new Predicate(">=", Literal.ofInt(0), new Column("a"))
        /*
        "NOT a = 1",
        "NOT a <=> 1",
        "a <=> 2",
        "a != 1",
        "2 <=> a",
        "1 != a",
         */
      )
    )
  }

  test("data skipping - nested, single 1") {
    checkSkipping(
      goldenTablePath("data-skipping-nested-single-1"),
      hits = Seq(
        new Predicate("=", new Column(Array("a", "b")), Literal.ofInt(1)),
        new Predicate(">=", new Column(Array("a", "b")), Literal.ofInt(1)),
        new Predicate("<=", new Column(Array("a", "b")), Literal.ofInt(1)),
        new Predicate("<=", new Column(Array("a", "b")), Literal.ofInt(2)),
        new Predicate(">=", new Column(Array("a", "b")), Literal.ofInt(0))
      ),
      misses = Seq(
        new Predicate("=", new Column(Array("a", "b")), Literal.ofInt(2)),
        new Predicate(">", new Column(Array("a", "b")), Literal.ofInt(1)),
        new Predicate("<", new Column(Array("a", "b")), Literal.ofInt(1))
      )
    )
  }

  test("data skipping - double nested, single 1") {
    checkSkipping(
      goldenTablePath("data-skipping-double-nested-single-1"),
      hits = Seq(
        new Predicate("=", new Column(Array("a", "b", "c")), Literal.ofInt(1)),
        new Predicate(">=", new Column(Array("a", "b", "c")), Literal.ofInt(1)),
        new Predicate("<=", new Column(Array("a", "b", "c")), Literal.ofInt(1)),
        new Predicate("<=", new Column(Array("a", "b", "c")), Literal.ofInt(2)),
        new Predicate(">=", new Column(Array("a", "b", "c")), Literal.ofInt(0))
      ),
      misses = Seq(
        new Predicate("=", new Column(Array("a", "b", "c")), Literal.ofInt(2)),
        new Predicate(">", new Column(Array("a", "b", "c")), Literal.ofInt(1)),
        new Predicate("<", new Column(Array("a", "b", "c")), Literal.ofInt(1))
      )
    )
  }

  private def longString(str: String) = str * 1000

  test("data skipping - long strings - long min") {
    checkSkipping(
      goldenTablePath("data-skipping-long-strings-long-min"),
      hits = Seq(
        new Predicate("=", new Column("a"), Literal.ofString(longString("A"))),
        new Predicate(">", new Column("a"), Literal.ofString("BA")),
        new Predicate("<", new Column("a"), Literal.ofString("AB"))
        /*
        "a like 'A%'",
         */
      ),
      misses = Seq(
        new Predicate("<", new Column("a"), Literal.ofString("AA")),
        new Predicate(">", new Column("a"), Literal.ofString("CD"))
      )
    )
  }

  test("data skipping - long strings - long max") {
    checkSkipping(
      goldenTablePath("data-skipping-long-strings-long-max"),
      hits = Seq(
        new Predicate("=", new Column("a"), Literal.ofString(longString("C"))),
        new Predicate(">", new Column("a"), Literal.ofString("BA")),
        new Predicate("<", new Column("a"), Literal.ofString("AB")),
        new Predicate(">", new Column("a"), Literal.ofString("CC"))
        /*
        "a like 'A%'",
        "a like 'C%'",
         */
      ),
      misses = Seq(
        new Predicate(">=", new Column("a"), Literal.ofString("D")),
        new Predicate(">", new Column("a"), Literal.ofString("CD"))
      )
    )
  }

  // Test:'starts with'  Expression: like
  // Test:'starts with, nested'  Expression: like

  test("data skipping - and statements - simple") {
    checkSkipping(
      goldenTablePath("data-skipping-and-statements-simple"),
      hits = Seq(
        new And(
          new Predicate(">", new Column("a"), Literal.ofInt(0)),
          new Predicate("<", new Column("a"), Literal.ofInt(3)),
        ),
        new And(
          new Predicate("<=", new Column("a"), Literal.ofInt(1)),
          new Predicate(">", new Column("a"), Literal.ofInt(-1)),
        )
      ),
      misses = Seq(
        new And(
          new Predicate("<", new Column("a"), Literal.ofInt(0)),
          new Predicate(">", new Column("a"), Literal.ofInt(-2)),
        )
      )
    )
  }

  test("data skipping - and statements - two fields") {
    checkSkipping(
      goldenTablePath("data-skipping-and-statements-two-fields"),
      hits = Seq(
        new And(
          new Predicate(">", new Column("a"), Literal.ofInt(0)),
          new Predicate("=", new Column("b"), Literal.ofString("2017-09-01"))
        ),
        new And(
          new Predicate("=", new Column("a"), Literal.ofInt(2)),
          new Predicate(">=", new Column("b"), Literal.ofString("2017-08-30"))
        )
        // "a >= 2 AND b like '2017-08-%'"
      ),
      misses = Seq(
        // "a > 0 AND b like '2016-%'"
      )
    )
  }

  test("data skipping - and statements - one side unsupported") {
    checkSkipping(
      goldenTablePath("data-skipping-and-statements-one-side-unsupported"),
      hits = Seq(
        new And(
          new Predicate("<",
            new ScalarExpression("%", Seq(new Column("a"), Literal.ofInt(100)).asJava),
            Literal.ofInt(10)),
          new Predicate(">",
            new ScalarExpression("%", Seq(new Column("b"), Literal.ofInt(100)).asJava),
            Literal.ofInt(20))
        ),
      ),
      misses = Seq(
        new And(
          new Predicate("<", new Column("a"), Literal.ofInt(10)),
          new Predicate(">",
            new ScalarExpression("%", Seq(new Column("b"), Literal.ofInt(100)).asJava),
            Literal.ofInt(20))
        ),
        new And(
          new Predicate("<",
            new ScalarExpression("%", Seq(new Column("a"), Literal.ofInt(100)).asJava),
            Literal.ofInt(10)),
          new Predicate(">", new Column("b"), Literal.ofInt(20))
        )
      )
    )
  }

  // Test: 'or statements - simple' Expression: OR
  // Test: 'or statements - two fields' Expression: OR
  // Test: 'or statements - one side supported' Expression: OR
  // Test: 'not statements - simple' Expression: NOT
  // Test: 'NOT statements - and' Expression: NOT
  // Test: 'NOT statements - or' Expression: NOT, OR

  // todo "Missing stats columns"
  // todo indexed col tests L399-L661

  test("data skipping - boolean comparisons") {
    checkSkipping(
      goldenTablePath("data-skipping-boolean-comparisons"),
      hits = Seq(
        new Predicate("=", new Column("a"), AlwaysFalse.ALWAYS_FALSE),
        new Predicate(">", new Column("a"), AlwaysTrue.ALWAYS_TRUE),
        new Predicate("<=", new Column("a"), AlwaysFalse.ALWAYS_FALSE),
        new Predicate("=", AlwaysTrue.ALWAYS_TRUE, new Column("a")),
        new Predicate("<", AlwaysTrue.ALWAYS_TRUE, new Column("a")),
        /*
        "a = false",
        "NOT a = false",
        "false = a or a"
         */
      ),
      misses = Seq()
    )
  }

  // todo "nulls - only null in file"
  // todo "nulls - null + not-null in same file"
  // todo "data skipping by partitions and data values - nulls"
  // todo "data skipping on $timestampType"
  // todo "Basic: Data skipping with delta statistic column $label"?
  // todo "Data skipping with delta statistic column rename column"?
  // todo "Data skipping with delta statistic column drop column"?

  // Other testing

  // Tests based on code written
  // Skip on array columns for null_count stats
  // Test all data types
  // Implicit casting vs correct data types
  // Filter on non-existent columns
  // Filter on incompatible data type?
  // Filter on partition AND data column

  // Test with tightBound = true/false; test with DV
  // Test with column mapping (id and name?)
  // Changing which stats are collected between AddFiles (missing stats)
  // Incompatible readSchema

  test("don't read stats column when there is no usable data skipping filter") {
    // todo Remove this golden table when updating this test to read real stats
    val path = goldenTablePath("conditionally-read-add-stats")
    val tableClient = tableClientDisallowedStatsReads

    def snapshot(tableClient: TableClient): Snapshot = {
      Table.forPath(tableClient, path).getLatestSnapshot(tableClient)
    }

    def verifyNoStatsColumn(scanFiles: CloseableIterator[FilteredColumnarBatch]): Unit = {
      scanFiles.forEach { batch =>
        val addSchema = batch.getData.getSchema.get("add").getDataType.asInstanceOf[StructType]
        assert(addSchema.indexOf("stats") < 0)
      }
    }

    // no filter --> don't read stats
    verifyNoStatsColumn(
      snapshot(tableClientDisallowedStatsReads)
        .getScanBuilder(tableClient).build()
        .getScanFiles(tableClient))

    // partition filter only --> don't read stats
    val partFilter = new Predicate("=", new Column("part"), Literal.ofInt(1))
    verifyNoStatsColumn(
      snapshot(tableClientDisallowedStatsReads)
        .getScanBuilder(tableClient).withFilter(tableClient, partFilter).build()
        .getScanFiles(tableClient))

    // no eligible data skipping filter --> don't read stats
    // todo non-eligible expression
  }
}

object ScanSuite {

  private def throwErrorIfAddStatsInSchema(readSchema: StructType): Unit = {
    if (readSchema.indexOf("add") >= 0) {
      val addSchema = readSchema.get("add").getDataType.asInstanceOf[StructType]
      assert(addSchema.indexOf("stats") < 0, "reading column add.stats is not allowed");
    }
  }

  /**
   * Returns a custom table client implementation that doesn't allow "add.stats" in the read schema
   * for parquet or json handlers.
   */
  def tableClientDisallowedStatsReads: TableClient = {
    val hadoopConf = new Configuration()
    new DefaultTableClient(hadoopConf) {

      override def getParquetHandler: ParquetHandler = {
        new DefaultParquetHandler(hadoopConf) {
          override def readParquetFiles(
            fileIter: CloseableIterator[FileReadContext],
            physicalSchema: StructType): CloseableIterator[FileDataReadResult] = {
            throwErrorIfAddStatsInSchema(physicalSchema)
            super.readParquetFiles(fileIter, physicalSchema)
          }
        }
      }

      override def getJsonHandler: JsonHandler = {
        new DefaultJsonHandler(hadoopConf) {
          override def readJsonFiles(
            fileIter: CloseableIterator[FileReadContext],
            physicalSchema: StructType): CloseableIterator[FileDataReadResult] = {
            throwErrorIfAddStatsInSchema(physicalSchema)
            super.readJsonFiles(fileIter, physicalSchema)
          }
        }
      }
    }
  }
}
