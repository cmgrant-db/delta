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

import java.math.{BigDecimal => JBigDecimal}
import java.sql.Date
import java.time.{Instant, OffsetDateTime}
import java.time.temporal.ChronoUnit

import scala.collection.JavaConverters._

import io.delta.golden.GoldenTableUtils.goldenTablePath
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SparkSession, Row => SparkRow}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog}
import org.apache.spark.sql.types.{StructType => SparkStructType}
import org.scalatest.funsuite.AnyFunSuite

import io.delta.kernel.client.{FileReadContext, JsonHandler, ParquetHandler, TableClient}
import io.delta.kernel.data.{FileDataReadResult, FilteredColumnarBatch, Row}
import io.delta.kernel.expressions.{AlwaysFalse, AlwaysTrue, And, Column, Or, Predicate, ScalarExpression}
import io.delta.kernel.expressions.Literal._
import io.delta.kernel.types.StructType
import io.delta.kernel.types.StringType.STRING
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.utils.CloseableIterator
import io.delta.kernel.{Snapshot, Table}
import io.delta.kernel.internal.util.InternalUtils
import io.delta.kernel.internal.InternalScanFileUtils

import io.delta.kernel.defaults.client.{DefaultJsonHandler, DefaultParquetHandler, DefaultTableClient}
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestUtils}

class ScanSuite extends AnyFunSuite with TestUtils with ExpressionTestUtils with SQLHelper {
  import io.delta.kernel.defaults.ScanSuite._

  private val spark = SparkSession
    .builder()
    .appName("Spark Test Writer for Delta Kernel")
    .config("spark.master", "local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
  // scalastyle:off sparkimplicits
  import spark.implicits._
  // scalastyle:on sparkimplicits

  private def getDataSkippingConfs(
    indexedCols: Option[Int], deltaStatsColNamesOpt: Option[String]): Seq[(String, String)] = {
    val numIndexedColsConfOpt = indexedCols
      .map(DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.defaultTablePropertyKey -> _.toString)
    val indexedColNamesConfOpt = deltaStatsColNamesOpt
      .map(DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.defaultTablePropertyKey -> _)
    (numIndexedColsConfOpt ++ indexedColNamesConfOpt).toSeq
  }

  def writeDataSkippingTable(
    tablePath: String,
    data: String,
    schema: SparkStructType = null,
    indexedCols: Option[Int] = None,
    deltaStatsColNamesOpt: Option[String] = None): Unit = {
    withSQLConf(getDataSkippingConfs(indexedCols, deltaStatsColNamesOpt): _*) {
      val jsonRecords = data.split("\n").toSeq
      val reader = spark.read
      if (schema != null) { reader.schema(schema) }
      val df = reader.json(jsonRecords.toDS())

      val r = DeltaLog.forTable(spark, tablePath)
      df.coalesce(1).write.format("delta").save(r.dataPath.toString)
    }
  }
  // todo write tables in this test suite instead of using golden tables (use above methods)

  private def getScanFileStats(scanFiles: Seq[Row]): Seq[String] = {
    scanFiles.map { scanFile =>
      val addFile = scanFile.getStruct(scanFile.getSchema.indexOf("add"))
      if (scanFile.getSchema.indexOf("stats") >= 0) {
        addFile.getString(scanFile.getSchema.indexOf("stats"))
      } else {
        "[No stats read]"
      }
    }
  }

  /**
   * @param tablePath the table to scan
   * @param hits query filters that should yield at least one scan file
   * @param misses query filters that should yield no scan files
   */
  def checkSkipping(tablePath: String, hits: Seq[Predicate], misses: Seq[Predicate]): Unit = {
    val snapshot = latestSnapshot(tablePath)
    hits.foreach { predicate =>
      val scanFiles = collectScanFileRows(
        snapshot.getScanBuilder(defaultTableClient)
          .withFilter(defaultTableClient, predicate)
          .build())
      assert(scanFiles.nonEmpty, s"Expected hit but got miss for $predicate")
    }
    misses.foreach { predicate =>
      val scanFiles = collectScanFileRows(
        snapshot.getScanBuilder(defaultTableClient)
          .withFilter(defaultTableClient, predicate)
          .build())
      assert(scanFiles.isEmpty, s"Expected miss but got hit for $predicate\n" +
        s"Returned scan files have stats: ${getScanFileStats(scanFiles)}"
      )
    }
  }

  /**
   * @param tablePath the table to scan
   * @param filterToNumExpFiles map of {predicate -> number of expected scan files}
   */
  def checkSkipping(tablePath: String, filterToNumExpFiles: Map[Predicate, Int]): Unit = {
    val snapshot = latestSnapshot(tablePath)
    filterToNumExpFiles.foreach { case (filter, numExpFiles) =>
      val scanFiles = collectScanFileRows(
        snapshot.getScanBuilder(defaultTableClient)
          .withFilter(defaultTableClient, filter)
          .build())
      assert(scanFiles.length == numExpFiles,
        s"Expected $numExpFiles but found ${scanFiles.length} for $filter")
    }
  }

  /* Where timestampStr is in the format of "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" */
  def getTimestampPredicate(expr: String, col: Column, timestampStr: String): Predicate = {
    val time = OffsetDateTime.parse(timestampStr)
    new Predicate(expr, col, ofTimestamp(ChronoUnit.MICROS.between(Instant.EPOCH, time)))
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Skipping tests from Spark's DataSkippingDeltaTests
  //////////////////////////////////////////////////////////////////////////////////

  test("data skipping - top level, single 1") {
    checkSkipping(
      goldenTablePath("data-skipping-top-level-single-1"),
      hits = Seq(
        AlwaysTrue.ALWAYS_TRUE, // trivial base case
        equals(col("a"), ofInt(1)), // a = 1
        equals(ofInt(1), col("a")), // 1 = a
        greaterThanOrEqual(col("a"), ofInt(1)), // a >= 1
        lessThanOrEqual(col("a"), ofInt(1)), // a <= 1
        lessThanOrEqual(col("a"), ofInt(2)), // a <= 2
        greaterThanOrEqual(col("a"), ofInt(0)), // a >= 0
        lessThanOrEqual(ofInt(1), col("a")), // 1 <= a
        greaterThanOrEqual(ofInt(1), col("a")), // 1 >= a
        greaterThanOrEqual(ofInt(2), col("a")), // 2 >= a
        lessThanOrEqual(ofInt(0), col("a")), // 0 <= a
        // note <=> is not supported yet but these should still be hits once supported
        nullSafeEquals(col("a"), ofInt(1)), // a <=> 1
        nullSafeEquals(ofInt(1), col("a")), // 1 <=> a
        not(nullSafeEquals(col("a"), ofInt(2))), // NOT a <=> 2
        // MOVE BELOW EXPRESSIONS TO MISSES ONCE SUPPORTED BY DATA SKIPPING
        not(equals(col("a"), ofInt(1))), // NOT a = 1
        not(nullSafeEquals(col("a"), ofInt(1))), // NOT a <=> 1
        nullSafeEquals(col("a"), ofInt(2)), // a <=> 2
        notEquals(col("a"), ofInt(1)), // a != 1
        nullSafeEquals(col("a"), ofInt(2)), // a <=> 2
        notEquals(ofInt(1), col("a")) // 1 != a
      ),
      misses = Seq(
        equals(col("a"), ofInt(2)), // a = 2
        equals(ofInt(2), col("a")), // 2 = a
        greaterThan(col("a"), ofInt(1)), // a > 1
        lessThan(col("a"), ofInt(1)), // a  < 1
        greaterThanOrEqual(col("a"), ofInt(2)), // a >= 2
        lessThanOrEqual(col("a"), ofInt(0)), // a <= 0
        lessThan(ofInt(1), col("a")), // 1 < a
        greaterThan(ofInt(1), col("a")), // 1 > a
        lessThanOrEqual(ofInt(2), col("a")), // 2 <= a
        greaterThanOrEqual(ofInt(0), col("a")) // 0 >= a
      )
    )
  }

  test("data skipping - nested, single 1") {
    checkSkipping(
      goldenTablePath("data-skipping-nested-single-1"),
      hits = Seq(
        equals(nestedCol("a.b"), ofInt(1)), // a.b = 1
        greaterThanOrEqual(nestedCol("a.b"), ofInt(1)), // a.b >= 1
        lessThanOrEqual(nestedCol("a.b"), ofInt(1)), // a.b <= 1
        lessThanOrEqual(nestedCol("a.b"), ofInt(2)), // a.b <= 2
        greaterThanOrEqual(nestedCol("a.b"), ofInt(0)) // a.b >= 0
      ),
      misses = Seq(
        equals(nestedCol("a.b"), ofInt(2)), // a.b = 2
        greaterThan(nestedCol("a.b"), ofInt(1)), // a.b > 1
        lessThan(nestedCol("a.b"), ofInt(1)) // a.b < 1
      )
    )
  }

  test("data skipping - double nested, single 1") {
    checkSkipping(
      goldenTablePath("data-skipping-double-nested-single-1"),
      hits = Seq(
        equals(nestedCol("a.b.c"), ofInt(1)), // a.b.c = 1
        greaterThanOrEqual(nestedCol("a.b.c"), ofInt(1)), // a.b.c >= 1
        lessThanOrEqual(nestedCol("a.b.c"), ofInt(1)), // a.b.c <= 1
        lessThanOrEqual(nestedCol("a.b.c"), ofInt(2)), // a.b.c <= 2
        greaterThanOrEqual(nestedCol("a.b.c"), ofInt(0)) // a.b.c >= 0
      ),
      misses = Seq(
        equals(nestedCol("a.b.c"), ofInt(2)), // a.b.c = 2
        greaterThan(nestedCol("a.b.c"), ofInt(1)), // a.b.c > 1
        lessThan(nestedCol("a.b.c"), ofInt(1)) // a.b.c < 1
      )
    )
  }

  private def longString(str: String) = str * 1000

  test("data skipping - long strings - long min") {
    checkSkipping(
      goldenTablePath("data-skipping-long-strings-long-min"),
      hits = Seq(
        equals(col("a"), ofString(longString("A"))),
        greaterThan(col("a"), ofString("BA")),
        lessThan(col("a"), ofString("AB")),
        // note startsWith is not supported yet but these should still be hits once supported
        startsWith(col("a"), ofString("A")) // a like 'A%'
      ),
      misses = Seq(
        lessThan(col("a"), ofString("AA")),
        greaterThan(col("a"), ofString("CD"))
      )
    )
  }

  test("data skipping - long strings - long max") {
    checkSkipping(
      goldenTablePath("data-skipping-long-strings-long-max"),
      hits = Seq(
        equals(col("a"), ofString(longString("C"))),
        greaterThan(col("a"), ofString("BA")),
        lessThan(col("a"), ofString("AB")),
        greaterThan(col("a"), ofString("CC")),
        // note startsWith is not supported yet but these should still be hits once supported
        startsWith(col("a"), ofString("A")), // a like 'A%'
        startsWith(col("a"), ofString("C")) // a like 'C%'
      ),
      misses = Seq(
        greaterThanOrEqual(col("a"), ofString("D")),
        greaterThan(col("a"), ofString("CD"))
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
          greaterThan(col("a"), ofInt(0)),
          lessThan(col("a"), ofInt(3))
        ),
        new And(
          lessThanOrEqual(col("a"), ofInt(1)),
          greaterThan(col("a"), ofInt(-1))
        )
      ),
      misses = Seq(
        new And(
          lessThan(col("a"), ofInt(0)),
          greaterThan(col("a"), ofInt(-2))
        )
      )
    )
  }

  test("data skipping - and statements - two fields") {
    checkSkipping(
      goldenTablePath("data-skipping-and-statements-two-fields"),
      hits = Seq(
        new And(
          greaterThan(col("a"), ofInt(0)),
          equals(col("b"), ofString("2017-09-01"))
        ),
        new And(
          equals(col("a"), ofInt(2)),
          greaterThanOrEqual(col("b"), ofString("2017-08-30"))
        ),
        // note startsWith is not supported yet but these should still be hits once supported
        new And( //  a >= 2 AND b like '2017-08-%'
          greaterThanOrEqual(col("a"), ofInt(2)),
          startsWith(col("b"), ofString("2017-08-"))
        ),
        // MOVE BELOW EXPRESSION TO MISSES ONCE SUPPORTED BY DATA SKIPPING
        new And( // a > 0 AND b like '2016-%'
          greaterThan(col("a"), ofInt(0)),
          startsWith(col("b"), ofString("2016-"))
        )
      ),
      misses = Seq()
    )
  }

  test("data skipping - and statements - one side unsupported") {
    val aRem100 = new ScalarExpression("%", Seq(col("a"), ofInt(100)).asJava)
    val bRem100 = new ScalarExpression("%", Seq(col("b"), ofInt(100)).asJava)
    checkSkipping(
      goldenTablePath("data-skipping-and-statements-one-side-unsupported"),
      hits = Seq(
        // a % 100 < 10 AND b % 100 > 20
        new And(lessThan(aRem100, ofInt(10)), greaterThan(bRem100, ofInt(20)))
      ),
      misses = Seq(
        // a < 10 AND b % 100 > 20
        new And(lessThan(col("a"), ofInt(10)), greaterThan(bRem100, ofInt(20))),
        // a % 100 < 10 AND b > 20
        new And(lessThan(aRem100, ofInt(10)), greaterThan(col("b"), ofInt(20)))
      )
    )
  }

  // Test: 'or statements - simple' Expression: OR
  // Test: 'or statements - two fields' Expression: OR
  // Test: 'or statements - one side supported' Expression: OR
  // Test: 'not statements - simple' Expression: NOT
  // Test: 'NOT statements - and' Expression: NOT
  // Test: 'NOT statements - or' Expression: NOT, OR

  // If a column does not have stats, it does not participate in data skipping, which disqualifies
  // that leg of whatever conjunct it was part of.
  test("data skipping - missing stats columns") {
    checkSkipping(
      goldenTablePath("data-skipping-missing-stats-columns"),
      hits = Seq(
        lessThan(col("b"), ofInt(10)), // b < 10: disqualified
        // note OR is not supported yet but these should still be hits once supported
        new Or( // a < 1 OR b < 10: a disqualified by b (same conjunct)
          lessThan(col("a"), ofInt(1)), lessThan(col("b"), ofInt(10))),
        new Or( // a < 1 OR (a >= 1 AND b < 10): ==> a < 1 OR a >=1 ==> TRUE
          lessThan(col("a"), ofInt(1)),
          new And(greaterThanOrEqual(col("a"), ofInt(1)), lessThan(col("b"), ofInt(10)))
        ),
        // MOVE BELOW EXPRESSION TO MISSES ONCE SUPPORTED BY DATA SKIPPING
        new Or( // a < 1 OR (a > 10 AND b < 10): ==> a < 1 OR a > 10 ==> FALSE
          lessThan(col("a"), ofInt(1)),
          new And(greaterThan(col("a"), ofInt(10)), lessThan(col("b"), ofInt(10)))
        )
      ),
      misses = Seq(
        new And( // a < 1 AND b < 10: ==> a < 1 ==> FALSE
          lessThan(col("a"), ofInt(1)), lessThan(col("b"), ofInt(10)))
      )
    )
  }

  test("data-skipping - more columns than indexed") {
    checkSkipping(
      goldenTablePath("data-skipping-more-columns-than-indexed"),
      hits = Seq(
        equals(col("col00"), ofInt(0)),
        equals(col("col32"), ofInt(32)),
        equals(col("col32"), ofInt(-1))
      ),
      misses = Seq(
        equals(col("col00"), ofInt(1))
      )
    )
  }

  test("data skipping - nested schema - # indexed column = 3") {
    checkSkipping(
      goldenTablePath("data-skipping-nested-schema-3-indexed-column"),
      hits = Seq(
        equals(col("a"), ofInt(1)), // a = 1
        equals(nestedCol("b.c.d"), ofInt(2)), // b.c.d = 2
        equals(nestedCol("b.c.e"), ofInt(3)), // b.c.e = 3
        // below matches due to missing stats
        lessThan(nestedCol("b.c.f.g"), ofInt(0)), // b.c.f.g < 0
        lessThan(nestedCol("b.c.f.i"), ofInt(0)), // b.c.f.i < 0
        lessThan(nestedCol("b.l"), ofInt(0)) // b.l < 0
      ),
      misses = Seq(
        lessThan(col("a"), ofInt(0)), // a < 0
        lessThan(nestedCol("b.c.d"), ofInt(0)), // b.c.d < 0
        lessThan(nestedCol("b.c.e"), ofInt(0)) // b.c.e < 0
      )
    )
  }

  test("data skipping - nested schema - # indexed column = 0") {
    checkSkipping(
      goldenTablePath("data-skipping-nested-schema-0-indexed-column"),
      hits = Seq(
        // all included due to missing stats
        lessThan(col("a"), ofInt(0)),
        lessThan(nestedCol("b.c.d"), ofInt(0)),
        lessThan(nestedCol("b.c.f.i"), ofInt(0)),
        lessThan(nestedCol("b.l"), ofInt(0)),
        lessThan(col("m"), ofInt(0))
      ),
      misses = Seq()
    )
  }

  test("data skipping - " +
    "indexed column names - naming a nested column indexes all leaf fields of that column") {
    checkSkipping(
      goldenTablePath("data-skipping-indexed-column-names-naming-a-nested-column"),
      hits = Seq(
        // these all have missing stats
        lessThan(col("a"), ofInt(0)),
        lessThan(nestedCol("b.l"), ofInt(0)),
        lessThan(col("m"), ofInt(0))
      ),
      misses = Seq(
        lessThan(nestedCol("b.c.d"), ofInt(0)),
        lessThan(nestedCol("b.c.e"), ofInt(0)),
        lessThan(nestedCol("b.c.f.g"), ofInt(0)),
        lessThan(nestedCol("b.c.f.h"), ofInt(0)),
        lessThan(nestedCol("b.c.f.i"), ofInt(0)),
        lessThan(nestedCol("b.c.j"), ofInt(0)),
        lessThan(nestedCol("b.c.k"), ofInt(0))
      )
    )
  }

  test("data skipping - indexed column names - index only a subset of leaf columns") {
    checkSkipping(
      goldenTablePath("data-skipping-indexed-column-names-index-only-a-subset-of-leaf-columns"),
      hits = Seq(
        // these all have missing stats
        lessThan(col("a"), ofInt(0)),
        lessThan(nestedCol("b.c.d"), ofInt(0)),
        lessThan(nestedCol("b.c.f.g"), ofInt(0)),
        lessThan(nestedCol("b.c.f.i"), ofInt(0)),
        lessThan(nestedCol("b.c.j"), ofInt(0)),
        lessThan(col("m"), ofInt(0))
      ),
      misses = Seq(
        lessThan(nestedCol("b.c.e"), ofInt(0)),
        lessThan(nestedCol("b.c.f.h"), ofInt(0)),
        lessThan(nestedCol("b.c.k"), ofInt(0)),
        lessThan(nestedCol("b.l"), ofInt(0))
      )
    )
  }

  test("data skipping - boolean comparisons") {
    checkSkipping(
      goldenTablePath("data-skipping-boolean-comparisons"),
      hits = Seq(
        equals(col("a"), ofBoolean(false)),
        greaterThan(col("a"), ofBoolean(true)),
        lessThanOrEqual(col("a"), ofBoolean(false)),
        equals(ofBoolean(true), col("a")),
        lessThan(ofBoolean(true), col("a")),
        // note NOT is not supported yet but these should still be hits once supported
        not(equals(col("a"), ofBoolean(false)))
      ),
      misses = Seq()
    )
  }

  // Data skipping by stats should still work even when the only data in file is null, in spite of
  // the NULL min/max stats that result -- this is different to having no stats at all.
  test("data skipping - nulls - only null in file") {
    checkSkipping(
      goldenTablePath("data-skipping-nulls-only-null-in-file"),
      hits = Seq(
        AlwaysTrue.ALWAYS_TRUE,
        // Ideally this should not hit as it is always FALSE, but its correct to not skip
        equals(col("a"), ofNull(INTEGER)),
        not(equals(col("a"), ofNull(INTEGER))), // Same as previous case
        isNull(col("a")),
        // This is optimized to `IsNull(a)` by NullPropagation in Spark
        nullSafeEquals(col("a"), ofNull(INTEGER)),
        not(nullSafeEquals(col("a"), ofInt(1))),
        // In delta-spark we use verifyStatsForFilter to deal with missing stats instead of
        // converting all nulls ==> true (keep). For comparisons with null statistics we end up with
        // filter: dataFilter || !(verifyStatsForFilter) = null || false = null
        // When filtering on a DF nulls are counted as false and eliminated. Thus these are misses
        // in Delta-Spark.
        // Including them is not incorrect. To skip these filters for Kernel we could use
        // verifyStatsForFilter or some other solution like inserting a && isNotNull(a) expression.
        equals(col("a"), ofInt(1)),
        lessThan(col("a"), ofInt(1)),
        greaterThan(col("a"), ofInt(1)),
        not(equals(col("a"), ofInt(1))),
        notEquals(col("a"), ofInt(1)),
        nullSafeEquals(col("a"), ofInt(1)),

        // MOVE BELOW EXPRESSIONS TO MISSES ONCE SUPPORTED BY DATA SKIPPING
        isNotNull(col("a")),
        // This can be optimized to `IsNotNull(a)` (done by NullPropagation in Spark)
        not(nullSafeEquals(col("a"), ofNull(INTEGER)))
      ),
      misses = Seq(
        AlwaysFalse.ALWAYS_FALSE
      )
    )
  }

  test("data skipping - nulls - null + not-null in same file") {
    checkSkipping(
      goldenTablePath("data-skipping-nulls-null-plus-not-null-in-same-file"),
      hits = Seq(
        // Ideally this should not hit as it is always FALSE, but its correct to not skip
        equals(col("a"), ofNull(INTEGER)),
        equals(col("a"), ofInt(1)),
        AlwaysTrue.ALWAYS_TRUE,
        isNotNull(col("a")),

        // Note these expressions either aren't supported or aren't added to skipping yet
        // but should still be hits once supported
        isNull(col("a")),
        not(equals(col("a"), ofNull(INTEGER))),
        // This is optimized to `IsNull(a)` by NullPropagation in Spark
        nullSafeEquals(col("a"), ofNull(INTEGER)),
        // This is optimized to `IsNotNull(a)` by NullPropagation in Spark
        not(nullSafeEquals(col("a"), ofNull(INTEGER))),
        nullSafeEquals(col("a"), ofInt(1)),
        not(nullSafeEquals(col("a"), ofInt(1))),

        // MOVE BELOW EXPRESSIONS TO MISSES ONCE SUPPORTED BY DATA SKIPPING
        notEquals(col("a"), ofInt(1)),
        not(equals(col("a"), ofInt(1)))

      ),
      misses = Seq(
        AlwaysFalse.ALWAYS_FALSE,
        lessThan(col("a"), ofInt(1)),
        greaterThan(col("a"), ofInt(1))
      )
    )
  }

  // TODO JSON serialization truncates to milliseconds, to safely skip for timestamp stats we need
  //   to add a millisecond to any max stat (requires time addition expression)
  ignore("data skipping - on TIMESTAMP type") {
    checkSkipping(
      goldenTablePath("data-skipping-on-TIMESTAMP"),
      hits = Seq(
        getTimestampPredicate(">=", col("ts"), "2019-09-09T01:02:03.456789-07:00"),
        getTimestampPredicate("<=", col("ts"), "2019-09-09T01:02:03.456789-07:00"),
        getTimestampPredicate(
          ">=", nestedCol("nested.ts"), "2019-09-09T01:02:03.456789-07:00"),
        getTimestampPredicate(
          "<=", nestedCol("nested.ts"), "2019-09-09T01:02:03.456789-07:00")
      ),
      misses = Seq(
        getTimestampPredicate(">=", col("ts"), "2019-09-09T01:02:03.457001-07:00"),
        getTimestampPredicate("<=", col("ts"), "2019-09-09T01:02:03.455999-07:00"),
        getTimestampPredicate(
          ">=", nestedCol("nested.ts"), "2019-09-09T01:02:03.457001-07:00"),
        getTimestampPredicate(
          "<=", nestedCol("nested.ts"), "2019-09-09T01:02:03.455999-07:00")
      )
    )
  }

  test("data skipping - Basic: Data skipping with delta statistic column") {
    checkSkipping(
      goldenTablePath("data-skipping-basic-data-skipping-with-delta-statistic-column"),
      hits = Seq(
        equals(col("c1"), ofInt(1)),
        equals(col("c2"), ofString("2")),
        lessThan(col("c3"), ofFloat(1.5f)),
        greaterThan(col("c4"), ofFloat(1.0F)),
        equals(col("c6"), ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2002-02-02")))),
        // Binary Column doesn't support delta statistics.
        equals(col("c7"), ofBinary("1111".getBytes)),
        equals(col("c7"), ofBinary("3333".getBytes)),
        equals(col("c8"), ofBoolean(true)),
        equals(col("c8"), ofBoolean(false)),
        greaterThan(col("c9"), ofDecimal(JBigDecimal.valueOf(1.5), 3, 2)),
        // TODO we don't currently skip for timestamps but this will still be a hit once we do
        getTimestampPredicate(">=", col("c5"), "2001-01-01T01:00:00-07:00"),
        // TODO once we skip for timestamps this should be a miss
        getTimestampPredicate(">=", col("c5"), "2003-01-01T01:00:00-07:00")
      ),
      misses = Seq(
        equals(col("c1"), ofInt(10)),
        equals(col("c2"), ofString("4")),
        lessThan(col("c3"), ofFloat(0.5f)),
        greaterThan(col("c4"), ofFloat(5.0f)),
        equals(col("c6"), ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2003-02-02")))),
        greaterThan(col("c9"), ofDecimal(JBigDecimal.valueOf(2.5), 3, 2))
      )
    )
  }

  test("data skipping - Data skipping with delta statistic column rename column") {
    checkSkipping(
      goldenTablePath("data-skipping-with-delta-statistic-column-rename-column"),
      hits = Seq(
        equals(col("cc1"), ofInt(1)),
        equals(col("cc2"), ofString("2")),
        lessThan(col("cc3"), ofFloat(1.5f)),
        greaterThan(col("cc4"), ofFloat(1.0f)),
        equals(col("cc6"), ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2002-02-02")))),
        // Binary Column doesn't support delta statistics.
        equals(col("cc7"), ofBinary("1111".getBytes)),
        equals(col("cc7"), ofBinary("3333".getBytes)),
        equals(col("cc8"), ofBoolean(true)),
        equals(col("cc8"), ofBoolean(false)),
        greaterThan(col("cc9"), ofDecimal(JBigDecimal.valueOf(1.5), 3, 2)),
        // We don't currently skip for timestamps but this will still be a hit once we do
        getTimestampPredicate(">=", col("cc5"), "2001-01-01T01:00:00-07:00"),
        // Once we skip for timestamps this should be a miss
        getTimestampPredicate(">=", col("cc5"), "2003-01-01T01:00:00-07:00")
      ),
      misses = Seq(
        equals(col("cc1"), ofInt(10)),
        equals(col("cc2"), ofString("4")),
        lessThan(col("cc3"), ofFloat(0.5f)),
        greaterThan(col("cc4"), ofFloat(5.0f)),
        equals(col("cc6"), ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2003-02-02")))),
        greaterThan(col("cc9"), ofDecimal(JBigDecimal.valueOf(2.5), 3, 2))
      )
    )
  }

  test("data skipping - Data skipping with delta statistic column drop column") {
    checkSkipping(
      goldenTablePath("data-skipping-with-delta-statistic-column-drop-column"),
      hits = Seq(
        equals(col("c1"), ofInt(1)),
        lessThan(col("c3"), ofFloat(1.5f)),
        greaterThan(col("c4"), ofFloat(1.0f)),
        equals(col("c6"), ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2002-02-02")))),
        greaterThan(col("c9"), ofDecimal(JBigDecimal.valueOf(1.5), 3, 2)),
        // We don't currently skip for timestamps but this will still be a hit once we do
        getTimestampPredicate(">=", col("c5"), "2001-01-01T01:00:00-07:00"),
        // Once we skip for timestamps this should be a miss
        getTimestampPredicate(">=", col("c5"), "2003-01-01T01:00:00-07:00")
      ),
      misses = Seq(
        equals(col("c1"), ofInt(10)),
        lessThan(col("c3"), ofFloat(0.5f)),
        greaterThan(col("c4"), ofFloat(5.0f)),
        equals(col("c6"), ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2003-02-02")))),
        greaterThan(col("c9"), ofDecimal(JBigDecimal.valueOf(2.5), 3, 2))
      )
    )
  }

  test("data skipping by partition and data values - nulls") {
    withTempDir { tableDir =>
      val dataSeqs = Seq( // each sequence produce a single file
        Seq((null, null)),
        Seq((null, "a")),
        Seq((null, "b")),
        Seq(("a", "a"), ("a", null)),
        Seq(("b", null))
      )
      dataSeqs.foreach { seq =>
        seq.toDF("key", "value").coalesce(1)
          .write.format("delta").partitionBy("key").mode("append").save(tableDir.getCanonicalPath)
      }
      def checkResults(
          predicate: Predicate, expNumPartitions: Int, expNumFiles: Long): Unit = {
        val snapshot = latestSnapshot(tableDir.getCanonicalPath)
        val scanFiles = collectScanFileRows(
          snapshot.getScanBuilder(defaultTableClient)
            .withFilter(defaultTableClient, predicate)
            .build())
        assert(scanFiles.length == expNumFiles,
          s"Expected $expNumFiles but found ${scanFiles.length} for $predicate")

        val partitionValues = scanFiles.map { row =>
          InternalScanFileUtils.getPartitionValues(row)
        }.distinct
        assert(partitionValues.length == expNumPartitions,
          s"Expected $expNumPartitions partitions but found ${partitionValues.length}")
      }

      // Trivial base case
      checkResults(
        predicate = AlwaysTrue.ALWAYS_TRUE,
        expNumPartitions = 3,
        expNumFiles = 5)

      // Conditions on partition key
      checkResults(
        predicate = isNotNull(col("key")),
        expNumPartitions = 2,
        expNumFiles = 2) // 2 files with key = 'a', and 1 file with key = 'b'

      checkResults(
        predicate = equals(col("key"), ofString("a")),
        expNumPartitions = 1,
        expNumFiles = 1) // 1 files with key = 'a'


      checkResults(
        predicate = equals(col("key"), ofString("b")),
        expNumPartitions = 1,
        expNumFiles = 1) // 1 files with key = 'b'

      // TODO shouldn't partition filters on unsupported expressions just not prune instead of fail?
      /*
      NOT YET SUPPORTED EXPRESSIONS
      checkResults(
        predicate = isNull(col("key")),
        expNumPartitions = 1,
        expNumFiles = 3) // 3 files with key = null

      checkResults(
        predicate = nullSafeEquals(col("key"), ofNull(string)),
        expNumPartitions = 1,
        expNumFiles = 3) // 3 files with key = null

      checkResults(
        predicate = nullSafeEquals(col("key"), ofString("a")),
        expNumPartitions = 1,
        expNumFiles = 1) // 1 files with key <=> 'a'

      checkResults(
        predicate = nullSafeEquals(col("key"), ofString("b")),
        expNumPartitions = 1,
        expNumFiles = 1) // 1 files with key <=> 'b'
        */

      // Conditions on partitions keys and values
      checkResults(
        predicate = isNull(col("value")),
        expNumPartitions = 3,
        expNumFiles = 5) // should be 3 once IS_NULL is supported

      checkResults(
        predicate = isNotNull(col("value")),
        expNumPartitions = 3, // should be 2 once IS_NOT_NULL is supported for data skipping
        expNumFiles = 5) // should be 5 once IS_NOT_NULL is supported for skipping

      checkResults(
        predicate = nullSafeEquals(col("value"), ofNull(STRING)),
        expNumPartitions = 3,
        expNumFiles = 5) // should be 3 once <=> is supported

      checkResults(
        predicate = equals(col("value"), ofString("a")),
        expNumPartitions = 3, // should be 2 if we can correctly skip "value = 'a'" for nulls
        expNumFiles = 4) // should be 2 if we can correctly skip "value = 'a'" for nulls

      checkResults(
        predicate = nullSafeEquals(col("value"), ofString("a")),
        expNumPartitions = 3, // should be 2 once <=> is supported
        expNumFiles = 5) // should be 2 once <=> is supported

      checkResults(
        predicate = notEquals(col("value"), ofString("a")),
        expNumPartitions = 3, // should be 1 once <> is supported
        expNumFiles = 5) // should be 1 once <> is supported

      checkResults(
        predicate = equals(col("value"), ofString("b")),
        expNumPartitions = 2, // should be 1 if we can correctly skip "value = 'b'" for nulls
        expNumFiles = 3) // should be 1 if we can correctly skip "value = 'a'" for nulls

      checkResults(
        predicate = nullSafeEquals(col("value"), ofString("b")),
        expNumPartitions = 3, // should be 1 once <=> is supported
        expNumFiles = 5) // should be 1 once <=> is supported

      // Conditions on both, partition keys and values
      /*
      NOT YET SUPPORTED EXPRESSIONS
      checkResults(
        predicate = new And(isNull(col("key")), equals(col("value"), ofString("a"))),
        expNumPartitions = 2,
        expNumFiles = 1) // only one file in the partition has (*, "a")

      checkResults(
        predicate = new And(nullSafeEquals(col("key"), ofNull(STRING)), nullSafeEquals(col("value"),
        ofNull(STRING))),
        expNumPartitions = 1,
        expNumFiles = 1) // 3 files with key = null, but only 1 with val = null.       */

      checkResults(
        predicate = new And(isNotNull(col("key")), isNotNull(col("value"))),
        expNumPartitions = 2, // should be 1 once we do data skipping for IS_NOT_NULL
        expNumFiles = 2) // should be 2 once we do data skipping for IS_NUT_NULL

      checkResults(
        predicate = new Or(
          nullSafeEquals(col("key"), ofNull(STRING)), nullSafeEquals(col("value"), ofNull(STRING))),
        expNumPartitions = 3,
        expNumFiles = 5) // all 5 files
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Kernel data skipping tests
  //////////////////////////////////////////////////////////////////////////////////

  test("basic data skipping for all types - all CM modes + checkpoint") {
    // Map of column name to (value_in_table, smaller_value, bigger_value)
    val colToLits = Map(
      "as_int" -> (ofInt(0), ofInt(-1), ofInt(1)),
      "as_long" -> (ofLong(0), ofLong(-1), ofLong(1)),
      "as_byte" -> (ofByte(0), ofByte(-1), ofByte(1)),
      "as_short" -> (ofShort(0), ofShort(-1), ofShort(1)),
      "as_float" -> (ofFloat(0), ofFloat(-1), ofFloat(1)),
      "as_double" -> (ofDouble(0), ofDouble(-1), ofDouble(1)),
      "as_string" -> (ofString("0"), ofString("!"), ofString("1")),
      "as_date" -> (ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2000-01-01"))),
        ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("1999-01-01"))),
        ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2000-01-02")))),
      // TODO add Timestamp once we support skipping for TimestampType
      "as_big_decimal" -> (ofDecimal(JBigDecimal.valueOf(0), 1, 0),
        ofDecimal(JBigDecimal.valueOf(-1), 1, 0),
        ofDecimal(JBigDecimal.valueOf(1), 1, 0))
    )
    val misses = colToLits.flatMap { case (colName, (value, small, big)) =>
      Seq(
        equals(col(colName), small),
        greaterThan(col(colName), value),
        greaterThanOrEqual(col(colName), big),
        lessThan(col(colName), value),
        lessThanOrEqual(col(colName), small)
      )
    }.toSeq
    val hits = colToLits.flatMap { case (colName, (value, small, big)) =>
      Seq(
        equals(col(colName), value),
        greaterThan(col(colName), small),
        greaterThanOrEqual(col(colName), value),
        lessThan(col(colName), big),
        lessThanOrEqual(col(colName), value)
      )
    }.toSeq
    Seq(
      "data-skipping-basic-stats-all-types",
      "data-skipping-basic-stats-all-types-columnmapping-name",
      "data-skipping-basic-stats-all-types-columnmapping-id",
      "data-skipping-basic-stats-all-types-checkpoint"
    ).foreach { goldenTable =>
      checkSkipping(
        goldenTablePath(goldenTable),
        hits,
        misses
      )
    }
  }

  test("data skipping - implicit casting works") {
    checkSkipping(
      goldenTablePath("data-skipping-basic-stats-all-types"),
      hits = Seq(
        equals(col("as_short"), ofFloat(0f)),
        equals(col("as_float"), ofShort(0))
      ),
      misses = Seq(
        equals(col("as_short"), ofFloat(1f)),
        equals(col("as_float"), ofShort(1))
      )
    )
  }

  test("data skipping - incompatible schema change doesn't break") {
    checkSkipping(
      goldenTablePath("data-skipping-incompatible-schema-change"),
      hits = Seq(
        equals(col("value"), ofString("1"))
      ),
      misses = Seq(
        equals(col("value"), ofString("3"))
      )
    )
  }

  test("data skipping - filter on non-existent column") {
    checkSkipping(
      goldenTablePath("data-skipping-basic-stats-all-types"),
      hits = Seq(equals(col("foo"), ofInt(1))),
      misses = Seq()
    )
  }

  // todo add a test with dvs where tightBounds=false

  test("data skipping - filter on partition AND data column") {
    checkSkipping(
      goldenTablePath("data-skipping-basic-stats-all-types"),
      filterToNumExpFiles = Map(
        new And(
          greaterThan(col("part"), ofInt(0)),
          greaterThan(col("id"), ofInt(0))
        ) -> 1 // should prune 3 files from partition + data filter
      )
    )
  }

  test("data skipping - stats collected changing across versions") {
    checkSkipping(
      goldenTablePath("data-skipping-change-stats-collected-across-versions"),
      filterToNumExpFiles = Map(
        equals(col("col1"), ofInt(1)) -> 1, // should prune 2 files
        equals(col("col2"), ofInt(1)) -> 2, // should prune 1 file
        new And(
          equals(col("col1"), ofInt(1)),
          equals(col("col2"), ofInt(1))
        ) -> 1 // should prune 2 files
      )
    )
  }

  test("data skipping - range of ints") {
    // to test where MIN != MAX
    checkSkipping(
      goldenTablePath("data-skipping-range-of-ints"),
      hits = Seq(
        equals(col("id"), ofInt(5)),
        lessThan(col("id"), ofInt(7)),
        lessThan(col("id"), ofInt(15)),
        lessThanOrEqual(col("id"), ofInt(9)),
        greaterThan(col("id"), ofInt(3)),
        greaterThan(col("id"), ofInt(-1)),
        greaterThanOrEqual(col("id"), ofInt(0))
      ),
      misses = Seq(
        equals(col("id"), ofInt(10)),
        lessThan(col("id"), ofInt(0)),
        lessThan(col("id"), ofInt(-1)),
        lessThanOrEqual(col("id"), ofInt(-1)),
        greaterThan(col("id"), ofInt(10)),
        greaterThan(col("id"), ofInt(11)),
        greaterThanOrEqual(col("id"), ofInt(11))
      )
    )
  }

  test("data skipping - non-eligible data skipping types") {
    withTempDir { tempDir =>
      val schema = SparkStructType.fromDDL("`id` INT, `arr_col` ARRAY<INT>, " +
        "`map_col` MAP<STRING, INT>, `struct_col` STRUCT<`field1`: INT>")
      val data = SparkRow(0, Array(1, 2), Map("foo" -> 1), SparkRow(5)) :: Nil
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.format("delta").save(tempDir.getCanonicalPath)
      // For now just filter on the one eligible column to ensure we can read stats from tables with
      // these types. In the future we should be able to skip with null predicates on these columns
      checkSkipping(
        tempDir.getCanonicalPath,
        hits = Seq(equals(col("id"), ofInt(0))),
        misses = Seq(equals(col("id"), ofInt(1)))
      )
    }
  }

  test("don't read stats column when there is no usable data skipping filter") {
    val path = goldenTablePath("data-skipping-basic-stats-all-types")
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
    val partFilter = equals(new Column("part"), ofInt(1))
    verifyNoStatsColumn(
      snapshot(tableClientDisallowedStatsReads)
        .getScanBuilder(tableClient).withFilter(tableClient, partFilter).build()
        .getScanFiles(tableClient))

    // no eligible data skipping filter --> don't read stats
    val nonEligibleFilter = lessThan(
      new ScalarExpression("%", Seq(col("as_int"), ofInt(10)).asJava),
      ofInt(1))
    verifyNoStatsColumn(
      snapshot(tableClientDisallowedStatsReads)
        .getScanBuilder(tableClient).withFilter(tableClient, nonEligibleFilter).build()
        .getScanFiles(tableClient))
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
