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

package org.apache.spark.sql.delta.perf

import java.sql.Timestamp
import java.util.Locale

import scala.util.matching.Regex

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.sources.DeltaSQLConf.GENERATED_COLUMN_PARTITION_FILTER_OPTIMIZATION_ENABLED
import io.delta.tables.DeltaTable

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.util.Utils

trait OptimizeGeneratedColumnSuiteBase extends GeneratedColumnTest {
  import testImplicits._

  private val regex = new Regex(s"(\\S+)\\s(\\S+)\\sGENERATED\\sALWAYS\\sAS\\s\\((.*)\\s?\\)",
  "col_name", "data_type", "generated_as")

  private def getPushedPartitionFilters(queryExecution: QueryExecution): Seq[Expression] = {
    queryExecution.executedPlan.collectFirst {
      case scan: FileSourceScanExec => scan.partitionFilters
    }.getOrElse(Nil)
  }

  protected def createTable(
      tableName: String,
      normalColDDL: Seq[String],
      generatedPartColDDL: Seq[String],
      location: Option[String] = None): Unit = {
    val generatedPartCol = generatedPartColDDL.map(_.split(" ")(0))

    val tableBuilder = DeltaTable.create(spark).tableName(tableName)

    normalColDDL
      .map(_.split(" "))
      .foreach { normalCol =>
        tableBuilder.addColumn(normalCol(0), normalCol(1))
      }

   generatedPartColDDL.flatMap(regex.findFirstMatchIn(_))
      .foreach { m =>
        tableBuilder.addColumn(
          DeltaTable.columnBuilder(m.group("col_name"))
            .dataType(m.group("data_type"))
            .generatedAlwaysAs(m.group("generated_as"))
            .build())
      }

    tableBuilder.partitionedBy(generatedPartCol: _*)
    location.foreach(tableBuilder.location)

    tableBuilder.execute()
  }

  protected def insertInto(path: String, df: DataFrame) = {
    df.write.format("delta").mode("append").save(path)
  }

  /**
   * Verify we can recognize an `OptimizablePartitionExpression` and generate corresponding
   * partition filters correctly.
   *
   * @param normalColDDL DDL to define a data column
   * @param generatedPartColDDL a list of generated partition columns defined using the above data
   *                            column
   * @param expectedPartitionExpr the expected `OptimizablePartitionExpression` to be recognized
   * @param filterTestCases test cases for partition filters. The key is the data filter, and the
   *                        value is the partition filters we should generate.
   */
  private def testOptimizablePartitionExpression(
      normalColDDL: String,
      generatedPartColDDL: Seq[String],
      expectedPartitionExpr: OptimizablePartitionExpression,
      auxiliaryTestName: Option[String] = None,
      filterTestCases: Seq[(String, Seq[String])]): Unit = {
    test(expectedPartitionExpr.toString + auxiliaryTestName.getOrElse("")) {
      val normalCol = normalColDDL.split(" ")(0)

      withTableName("optimizable_partition_expression") { table =>
        createTable(table, Seq(normalColDDL), generatedPartColDDL)

        val metadata = DeltaLog.forTable(spark, TableIdentifier(table)).snapshot.metadata
        assert(metadata.optimizablePartitionExpressions(normalCol.toLowerCase(Locale.ROOT)) ==
          expectedPartitionExpr :: Nil)
        filterTestCases.foreach { filterTestCase =>
          val partitionFilters = getPushedPartitionFilters(
            sql(s"SELECT * from $table where ${filterTestCase._1}").queryExecution)
          assert(partitionFilters.map(_.sql) == filterTestCase._2)
        }
      }
    }
  }

  /** Format a human readable SQL filter into Spark's compact SQL format */
  private def compactFilter(filter: String): String = {
    filter.replaceAllLiterally("\n", "")
      .replaceAll("(?<=\\)) +(?=\\))", "")
      .replaceAll("(?<=\\() +(?=\\()", "")
      .replaceAll("\\) +OR +\\(", ") OR (")
      .replaceAll("\\) +AND +\\(", ") AND (")
  }

  testOptimizablePartitionExpression(
    normalColDDL = "eventTime TIMESTAMP",
    generatedPartColDDL = "date DATE GENERATED ALWAYS AS ( CAST(eventTime AS DATE) )" :: Nil,
    expectedPartitionExpr = DatePartitionExpr("date"),
    auxiliaryTestName = Option(" from cast(timestamp)"),
    filterTestCases = Seq(
      "eventTime < '2021-01-01 18:00:00'" ->
        Seq("((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "eventTime <= '2021-01-01 18:00:00'" ->
        Seq("((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "eventTime = '2021-01-01 18:00:00'" ->
        Seq("((date = CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date = CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "eventTime > '2021-01-01 18:00:00'" ->
        Seq("((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "eventTime >= '2021-01-01 18:00:00'" ->
        Seq("((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "eventTime is null" -> Seq("(date IS NULL)"),
      // Verify we can reverse the order
      "'2021-01-01 18:00:00' > eventTime" ->
        Seq("((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "'2021-01-01 18:00:00' >= eventTime" ->
        Seq("((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "'2021-01-01 18:00:00' = eventTime" ->
        Seq("((date = CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date = CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "'2021-01-01 18:00:00' < eventTime" ->
        Seq("((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "'2021-01-01 18:00:00' <= eventTime" ->
        Seq("((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      // Verify date type literal. In theory, the best filter should be date < DATE '2021-01-01'.
      // But Spark's analyzer converts eventTime < '2021-01-01' to
      // `eventTime` < TIMESTAMP '2021-01-01 00:00:00'. So it's the same as
      // eventTime < '2021-01-01 18:00:00' for `OptimizeGeneratedColumn`.
      "eventTime < '2021-01-01'" ->
        Seq("((date <= CAST(TIMESTAMP '2021-01-01 00:00:00' AS DATE)) " +
          "OR ((date <= CAST(TIMESTAMP '2021-01-01 00:00:00' AS DATE)) IS NULL))")
    )
  )

  testOptimizablePartitionExpression(
    normalColDDL = "eventDate DATE",
    generatedPartColDDL = "date DATE GENERATED ALWAYS AS ( CAST(eventDate AS DATE) )" :: Nil,
    expectedPartitionExpr = DatePartitionExpr("date"),
    auxiliaryTestName = Option(" from cast(date)"),
    filterTestCases = Seq(
      "eventDate < '2021-01-01 18:00:00'" ->
        Seq("((date <= DATE '2021-01-01') " +
          "OR ((date <= DATE '2021-01-01') IS NULL))"),
      "eventDate <= '2021-01-01 18:00:00'" ->
        Seq("((date <= DATE '2021-01-01') " +
          "OR ((date <= DATE '2021-01-01') IS NULL))"),
      "eventDate = '2021-01-01 18:00:00'" ->
        Seq("((date = DATE '2021-01-01') " +
          "OR ((date = DATE '2021-01-01') IS NULL))"),
      "eventDate > '2021-01-01 18:00:00'" ->
        Seq("((date >= DATE '2021-01-01') " +
          "OR ((date >= DATE '2021-01-01') IS NULL))"),
      "eventDate >= '2021-01-01 18:00:00'" ->
        Seq("((date >= DATE '2021-01-01') " +
          "OR ((date >= DATE '2021-01-01') IS NULL))"),
      "eventDate is null" -> Seq("(date IS NULL)"),
      // Verify we can reverse the order
      "'2021-01-01 18:00:00' > eventDate" ->
        Seq("((date <= DATE '2021-01-01') " +
          "OR ((date <= DATE '2021-01-01') IS NULL))"),
      "'2021-01-01 18:00:00' >= eventDate" ->
        Seq("((date <= DATE '2021-01-01') " +
          "OR ((date <= DATE '2021-01-01') IS NULL))"),
      "'2021-01-01 18:00:00' = eventDate" ->
        Seq("((date = DATE '2021-01-01') " +
          "OR ((date = DATE '2021-01-01') IS NULL))"),
      "'2021-01-01 18:00:00' < eventDate" ->
        Seq("((date >= DATE '2021-01-01') " +
          "OR ((date >= DATE '2021-01-01') IS NULL))"),
      "'2021-01-01 18:00:00' <= eventDate" ->
        Seq("((date >= DATE '2021-01-01') " +
          "OR ((date >= DATE '2021-01-01') IS NULL))"),
      // Verify date type literal. In theory, the best filter should be date < DATE '2021-01-01'.
      // But Spark's analyzer converts eventTime < '2021-01-01' to
      // `eventTime` < TIMESTAMP '2021-01-01 00:00:00'. So it's the same as
      // eventTime < '2021-01-01 18:00:00' for `OptimizeGeneratedColumn`.
      "eventDate < '2021-01-01'" ->
        Seq("((date <= DATE '2021-01-01') " +
          "OR ((date <= DATE '2021-01-01') IS NULL))")
    )
  )

  testOptimizablePartitionExpression(
    normalColDDL = "eventTime TIMESTAMP",
    generatedPartColDDL = Seq(
      "year INT GENERATED ALWAYS AS (YEAR(eventTime))",
      "month INT GENERATED ALWAYS AS (MONTH(eventTime))",
      "day INT GENERATED ALWAYS AS (DAY(eventTime))",
      "hour INT GENERATED ALWAYS AS (HOUR(eventTime))"
    ),
    expectedPartitionExpr = YearMonthDayHourPartitionExpr("year", "month", "day", "hour"),
    filterTestCases = Seq(
      "eventTime < '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (
            |    (
            |      (year < year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      OR
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month < month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |    )
            |    OR
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day < dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day = dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (hour <= hour(TIMESTAMP '2021-01-01 18:00:00'))
            |  )
            |)
            |""".stripMargin)),
      "eventTime <= '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (
            |    (
            |      (year < year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      OR
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month < month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |    )
            |    OR
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day < dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day = dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (hour <= hour(TIMESTAMP '2021-01-01 18:00:00'))
            |  )
            |)
            |""".stripMargin)),
      "eventTime = '2021-01-01 18:00:00'" -> Seq(
        "(year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))",
        "(month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))",
        "(day = dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))",
        "(hour = hour(TIMESTAMP '2021-01-01 18:00:00'))"
      ),
      "eventTime > '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (
            |    (
            |      (year > year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      OR
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month > month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |    )
            |    OR
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day > dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day = dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (hour >= hour(TIMESTAMP '2021-01-01 18:00:00'))
            |  )
            |)
            |""".stripMargin)),
      "eventTime >= '2021-01-01 18:00:00'" ->Seq(
        compactFilter(
          """(
            |  (
            |    (
            |      (year > year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      OR
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month > month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |    )
            |    OR
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day > dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day = dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (hour >= hour(TIMESTAMP '2021-01-01 18:00:00'))
            |  )
            |)
            |""".stripMargin)),
      "eventTime is null" -> Seq(
        "(year IS NULL)",
        "(month IS NULL)",
        "(day IS NULL)",
        "(hour IS NULL)"
      )
    )
  )

  testOptimizablePartitionExpression(
    normalColDDL = "eventTime TIMESTAMP",
    generatedPartColDDL = Seq(
      "year INT GENERATED ALWAYS AS (YEAR(eventTime))",
      "month INT GENERATED ALWAYS AS (MONTH(eventTime))",
      "day INT GENERATED ALWAYS AS (DAY(eventTime))"
    ),
    expectedPartitionExpr = YearMonthDayPartitionExpr("year", "month", "day"),
    filterTestCases = Seq(
      "eventTime < '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (
            |    (year < year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    OR
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month < month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (day <= dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime <= '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (
            |    (year < year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    OR
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month < month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (day <= dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime = '2021-01-01 18:00:00'" -> Seq(
        "(year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))",
        "(month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))",
        "(day = dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))"
      ),
      "eventTime > '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (
            |    (year > year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    OR
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month > month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (day >= dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime >= '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (
            |    (year > year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    OR
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month > month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (day >= dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime is null" -> Seq(
        "(year IS NULL)",
        "(month IS NULL)",
        "(day IS NULL)"
      )
    )
  )

  testOptimizablePartitionExpression(
    normalColDDL = "eventTime TIMESTAMP",
    generatedPartColDDL = Seq(
      // Use different cases to verify we can recognize the same column using different cases in
      // generation expressions.
      "year INT GENERATED ALWAYS AS (YEAR(EVENTTIME))",
      "month INT GENERATED ALWAYS AS (MONTH(eventtime))"
    ),
    expectedPartitionExpr = YearMonthPartitionExpr("year", "month"),
    filterTestCases = Seq(
      "eventTime < '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (year < year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  OR
            |  (
            |    (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    AND
            |    (month <= month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime <= '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (year < year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  OR
            |  (
            |    (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    AND
            |    (month <= month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime = '2021-01-01 18:00:00'" -> Seq(
        "(year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))",
        "(month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))"
      ),
      "eventTime > '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (year > year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  OR
            |  (
            |    (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    AND
            |    (month >= month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime >= '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (year > year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  OR
            |  (
            |    (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    AND
            |    (month >= month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime is null" -> Seq("(year IS NULL)", "(month IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    normalColDDL = "eventTime TIMESTAMP",
    generatedPartColDDL = "year INT GENERATED ALWAYS AS (YEAR(eventTime))" :: Nil,
    expectedPartitionExpr = YearPartitionExpr("year"),
    filterTestCases = Seq(
      "eventTime < '2021-01-01 18:00:00'" ->
        Seq("((year <= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) " +
          "OR ((year <= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) IS NULL))"),
      "eventTime <= '2021-01-01 18:00:00'" ->
        Seq("((year <= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) " +
          "OR ((year <= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) IS NULL))"),
      "eventTime = '2021-01-01 18:00:00'" ->
        Seq("((year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) " +
          "OR ((year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) IS NULL))"),
      "eventTime > '2021-01-01 18:00:00'" ->
        Seq("((year >= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) " +
          "OR ((year >= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) IS NULL))"),
      "eventTime >= '2021-01-01 18:00:00'" ->
        Seq("((year >= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) " +
          "OR ((year >= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) IS NULL))"),
      "eventTime is null" -> Seq("(year IS NULL)")
    )
  )

  Seq(("year INT GENERATED ALWAYS AS (YEAR(eventDate))", " from year(date)"),
    ("year INT GENERATED ALWAYS AS (YEAR(CAST(eventDate AS DATE)))", " from year(cast(date))"))
    .foreach { case (partColDDL, auxTestName) =>
      testOptimizablePartitionExpression(
        normalColDDL = "eventDate DATE",
        generatedPartColDDL = partColDDL :: Nil,
        expectedPartitionExpr = YearPartitionExpr("year"),
        auxiliaryTestName = Option(auxTestName),
        filterTestCases = Seq(
          "eventDate < '2021-01-01'" ->
            Seq("((year <= year(DATE '2021-01-01')) " +
              "OR ((year <= year(DATE '2021-01-01')) IS NULL))"),
          "eventDate <= '2021-01-01'" ->
            Seq("((year <= year(DATE '2021-01-01')) " +
              "OR ((year <= year(DATE '2021-01-01')) IS NULL))"),
          "eventDate = '2021-01-01'" ->
            Seq("((year = year(DATE '2021-01-01')) " +
              "OR ((year = year(DATE '2021-01-01')) IS NULL))"),
          "eventDate > '2021-01-01'" ->
            Seq("((year >= year(DATE '2021-01-01')) " +
              "OR ((year >= year(DATE '2021-01-01')) IS NULL))"),
          "eventDate >= '2021-01-01'" ->
            Seq("((year >= year(DATE '2021-01-01')) " +
              "OR ((year >= year(DATE '2021-01-01')) IS NULL))"),
          "eventDate is null" -> Seq("(year IS NULL)")
        )
      )
    }

  testOptimizablePartitionExpression(
    normalColDDL = "str STRING",
    generatedPartColDDL = "substr STRING GENERATED ALWAYS AS (SUBSTRING(str, 2, 3))" :: Nil,
    expectedPartitionExpr = SubstringPartitionExpr("substr", 2, 3),
    filterTestCases = Seq(
      "str < 'foo'" -> Nil,
      "str <= 'foo'" -> Nil,
      "str = 'foo'" -> Seq("((substr IS NULL) OR (substr = substring('foo', 2, 3)))"),
      "str > 'foo'" -> Nil,
      "str >= 'foo'" -> Nil,
      "str is null" -> Seq("(substr IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    normalColDDL = "str STRING",
    generatedPartColDDL = "substr STRING GENERATED ALWAYS AS (SUBSTRING(str, 0, 3))" :: Nil,
    expectedPartitionExpr = SubstringPartitionExpr("substr", 0, 3),
    filterTestCases = Seq(
      "str < 'foo'" -> Seq("((substr IS NULL) OR (substr <= substring('foo', 0, 3)))"),
      "str <= 'foo'" -> Seq("((substr IS NULL) OR (substr <= substring('foo', 0, 3)))"),
      "str = 'foo'" -> Seq("((substr IS NULL) OR (substr = substring('foo', 0, 3)))"),
      "str > 'foo'" -> Seq("((substr IS NULL) OR (substr >= substring('foo', 0, 3)))"),
      "str >= 'foo'" -> Seq("((substr IS NULL) OR (substr >= substring('foo', 0, 3)))"),
      "str is null" -> Seq("(substr IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    normalColDDL = "str STRING",
    generatedPartColDDL = "substr STRING GENERATED ALWAYS AS (SUBSTRING(str, 1, 3))" :: Nil,
    expectedPartitionExpr = SubstringPartitionExpr("substr", 1, 3),
    filterTestCases = Seq(
      "str < 'foo'" -> Seq("((substr IS NULL) OR (substr <= substring('foo', 1, 3)))"),
      "str <= 'foo'" -> Seq("((substr IS NULL) OR (substr <= substring('foo', 1, 3)))"),
      "str = 'foo'" -> Seq("((substr IS NULL) OR (substr = substring('foo', 1, 3)))"),
      "str > 'foo'" -> Seq("((substr IS NULL) OR (substr >= substring('foo', 1, 3)))"),
      "str >= 'foo'" -> Seq("((substr IS NULL) OR (substr >= substring('foo', 1, 3)))"),
      "str is null" -> Seq("(substr IS NULL)")
    )
  )

  test("end-to-end optimizable partition expression") {
    withTempDir { tempDir =>
      withTableName("optimizable_partition_expression") { table =>
        createTable(
          table,
          "c1 INT" :: "c2 TIMESTAMP" :: Nil,
          "c3 DATE GENERATED ALWAYS AS ( CAST(c2 AS DATE) )":: Nil,
          Some(tempDir.getCanonicalPath))
          Seq(
            Tuple2(1, "2020-12-31 11:00:00"),
            Tuple2(2, "2021-01-01 12:00:00"),
            Tuple2(3, "2021-01-02 13:00:00")
          ).foreach { values =>
            insertInto(
              tempDir.getCanonicalPath,
              Seq(values).toDF("c1", "c2")
                .withColumn("c2", $"c2".cast(TimestampType))
            )
          }
        assert(tempDir.listFiles().map(_.getName).toSet ==
          Set("c3=2021-01-01", "c3=2021-01-02", "c3=2020-12-31", "_delta_log"))
        // Delete folders which should not be read if we generate the partition filters correctly
        tempDir.listFiles().foreach { f =>
          if (f.getName != "c3=2021-01-01" && f.getName != "_delta_log") {
            Utils.deleteRecursively(f)
          }
        }
        assert(tempDir.listFiles().map(_.getName).toSet == Set("c3=2021-01-01", "_delta_log"))
        checkAnswer(
          sql(s"select * from $table where " +
            s"c2 >= '2021-01-01 12:00:00' AND c2 <= '2021-01-01 18:00:00'"),
          Row(2, sqlTimestamp("2021-01-01 12:00:00"), sqlDate("2021-01-01")))
        // Verify `OptimizeGeneratedColumn` doesn't mess up Projects.
        checkAnswer(
          sql(s"select c1 from $table where " +
            s"c2 >= '2021-01-01 12:00:00' AND c2 <= '2021-01-01 18:00:00'"),
          Row(2))

        // Check both projection orders to make sure projection orders are handled correctly
        checkAnswer(
          sql(s"select c1, c2 from $table where " +
            s"c2 >= '2021-01-01 12:00:00' AND c2 <= '2021-01-01 18:00:00'"),
          Row(2, Timestamp.valueOf("2021-01-01 12:00:00")))
        checkAnswer(
          sql(s"select c2, c1 from $table where " +
            s"c2 >= '2021-01-01 12:00:00' AND c2 <= '2021-01-01 18:00:00'"),
          Row(Timestamp.valueOf("2021-01-01 12:00:00"), 2))

        // Verify the optimization works for limit.
        val limitQuery = sql(
          s"""select * from $table
             |where c2 >= '2021-01-01 12:00:00' AND c2 <= '2021-01-01 18:00:00'
             |limit 10""".stripMargin)
        val expectedPartitionFilters = Seq(
          "((c3 >= CAST(TIMESTAMP '2021-01-01 12:00:00' AS DATE)) " +
            "OR ((c3 >= CAST(TIMESTAMP '2021-01-01 12:00:00' AS DATE)) IS NULL))",
          "((c3 <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
            "OR ((c3 <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"
        )
        assert(expectedPartitionFilters ==
          getPushedPartitionFilters(limitQuery.queryExecution).map(_.sql))
        checkAnswer(limitQuery, Row(2, sqlTimestamp("2021-01-01 12:00:00"), sqlDate("2021-01-01")))
      }
    }
  }

  test("empty string and null ambiguity in a partition column") {
    withTempDir { tempDir =>
      withTableName("optimizable_partition_expression") { table =>
        createTable(
          table,
          "c1 STRING" :: Nil,
          "c2 STRING GENERATED ALWAYS AS (SUBSTRING(c1, 1, 4))":: Nil,
          Some(tempDir.getCanonicalPath))
        insertInto(
          tempDir.getCanonicalPath,
          Seq(Tuple1("")).toDF("c1")
        )
        checkAnswer(
          sql(s"select * from $table where c1 = ''"),
          Row("", null))
        // The following check shows the weird behavior of SPARK-24438 and confirms the generated
        // partition filter doesn't impact the answer.
        withSQLConf(GENERATED_COLUMN_PARTITION_FILTER_OPTIMIZATION_ENABLED.key -> "false") {
          checkAnswer(
            sql(s"select * from $table where c1 = ''"),
            Row("", null))
        }
      }
    }
  }

  test("substring on multibyte characters") {
    withTempDir { tempDir =>
      withTableName("multibyte_characters") { table =>
        createTable(
          table,
          "c1 STRING" :: Nil,
          "c2 STRING GENERATED ALWAYS AS (SUBSTRING(c1, 1, 2))" :: Nil,
          Some(tempDir.getCanonicalPath))
        // scalastyle:off nonascii
        insertInto(
          tempDir.getCanonicalPath,
          Seq(Tuple1("一二三四")).toDF("c1")
        )
        val testQuery = s"select * from $table where c1 > 'abcd'"
        assert("((c2 IS NULL) OR (c2 >= substring('abcd', 1, 2)))" :: Nil ==
          getPushedPartitionFilters(sql(testQuery).queryExecution).map(_.sql))
        checkAnswer(
          sql(testQuery),
          Row("一二三四", "一二"))
        // scalastyle:on nonascii
      }
    }
  }

  testOptimizablePartitionExpression(
    normalColDDL = "eventTime TIMESTAMP",
    generatedPartColDDL =
      "month STRING GENERATED ALWAYS AS ((DATE_FORMAT(eventTime, 'yyyy-MM')))" :: Nil,
    expectedPartitionExpr = DateFormatPartitionExpr("month", "yyyy-MM"),
    auxiliaryTestName = Option(" from timestamp"),
    filterTestCases = Seq(
      "eventTime < '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') <= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') <= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), " +
          "'yyyy-MM')) IS NULL))"),
      "eventTime <= '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') <= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') <= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), " +
          "'yyyy-MM')) IS NULL))"),
      "eventTime = '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') = " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') = " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), " +
          "'yyyy-MM')) IS NULL))"),
      "eventTime > '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') >= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') >= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), " +
          "'yyyy-MM')) IS NULL))"),
      "eventTime >= '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') >= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') >= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), " +
          "'yyyy-MM')) IS NULL))"),
      "eventTime is null" -> Seq("(month IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    normalColDDL = "eventDate DATE",
    generatedPartColDDL =
      "month STRING GENERATED ALWAYS AS ((DATE_FORMAT(eventDate, 'yyyy-MM')))" :: Nil,
    expectedPartitionExpr = DateFormatPartitionExpr("month", "yyyy-MM"),
    auxiliaryTestName = Option(" from cast(date)"),
    filterTestCases = Seq(
      "eventDate < '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') <= unix_timestamp(date_format(CAST(" +
          "DATE '2021-06-28' AS TIMESTAMP), 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') <= " +
          "unix_timestamp(date_format(CAST(DATE '2021-06-28' AS TIMESTAMP), " +
          "'yyyy-MM'), 'yyyy-MM')) IS NULL))"),
      "eventDate <= '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') <= unix_timestamp(date_format(CAST(" +
          "DATE '2021-06-28' AS TIMESTAMP), 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') <= " +
          "unix_timestamp(date_format(CAST(DATE '2021-06-28' AS TIMESTAMP), " +
          "'yyyy-MM'), 'yyyy-MM')) IS NULL))"),
      "eventDate = '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') = unix_timestamp(date_format(CAST(" +
          "DATE '2021-06-28' AS TIMESTAMP), 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') = " +
          "unix_timestamp(date_format(CAST(DATE '2021-06-28' AS TIMESTAMP), " +
          "'yyyy-MM'), 'yyyy-MM')) IS NULL))"),
      "eventDate > '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') >= unix_timestamp(date_format(CAST(" +
          "DATE '2021-06-28' AS TIMESTAMP), 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') >= " +
          "unix_timestamp(date_format(CAST(DATE '2021-06-28' AS TIMESTAMP), " +
          "'yyyy-MM'), 'yyyy-MM')) IS NULL))"),
      "eventDate >= '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') >= unix_timestamp(date_format(CAST(" +
          "DATE '2021-06-28' AS TIMESTAMP), 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') >= " +
          "unix_timestamp(date_format(CAST(DATE '2021-06-28' AS TIMESTAMP), " +
          "'yyyy-MM'), 'yyyy-MM')) IS NULL))"),
      "eventDate is null" -> Seq("(month IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    normalColDDL = "eventTime TIMESTAMP",
    generatedPartColDDL =
      "hour STRING GENERATED ALWAYS AS ((DATE_FORMAT(eventTime, 'yyyy-MM-dd-HH')))" :: Nil,
    expectedPartitionExpr = DateFormatPartitionExpr("hour", "yyyy-MM-dd-HH"),
    filterTestCases = Seq(
      "eventTime < '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(hour, 'yyyy-MM-dd-HH') <= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) " +
          "OR ((unix_timestamp(hour, 'yyyy-MM-dd-HH') <= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', " +
          "'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) IS NULL))"),
      "eventTime <= '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(hour, 'yyyy-MM-dd-HH') <= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) " +
          "OR ((unix_timestamp(hour, 'yyyy-MM-dd-HH') <= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', " +
          "'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) IS NULL))"),
      "eventTime = '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(hour, 'yyyy-MM-dd-HH') = unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) " +
          "OR ((unix_timestamp(hour, 'yyyy-MM-dd-HH') = " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', " +
          "'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) IS NULL))"),
      "eventTime > '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(hour, 'yyyy-MM-dd-HH') >= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) " +
          "OR ((unix_timestamp(hour, 'yyyy-MM-dd-HH') >= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', " +
          "'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) IS NULL))"),
      "eventTime >= '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(hour, 'yyyy-MM-dd-HH') >= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) " +
          "OR ((unix_timestamp(hour, 'yyyy-MM-dd-HH') >= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', " +
          "'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) IS NULL))"),
      "eventTime is null" -> Seq("(hour IS NULL)")
    )
  )

  test("five digits year in a year month day partition column") {
    withTempDir { tempDir =>
      withTableName("optimizable_partition_expression") { table =>
        createTable(
          table,
          "c1 TIMESTAMP" :: Nil,
          Seq(
            "c2 INT GENERATED ALWAYS AS (YEAR(c1))",
            "c3 INT GENERATED ALWAYS AS (MONTH(c1))",
            "c4 INT GENERATED ALWAYS AS (DAY(c1))"),
          Some(tempDir.getCanonicalPath))
        insertInto(
          tempDir.getCanonicalPath,
          Seq(Tuple1("12345-07-15 18:00:00"))
            .toDF("c1")
            .withColumn("c1", $"c1".cast(TimestampType))
        )

        checkAnswer(
          sql(s"select * from $table where c1 = CAST('12345-07-15 18:00:00' as timestamp)"),
          Row(new Timestamp(327420320400000L), 12345, 7, 15))
        withSQLConf(GENERATED_COLUMN_PARTITION_FILTER_OPTIMIZATION_ENABLED.key -> "false") {
          checkAnswer(
            sql(s"select * from $table where c1 = CAST('12345-07-15 18:00:00' as timestamp)"),
            Row(new Timestamp(327420320400000L), 12345, 7, 15))
        }
      }
    }
  }

  test("five digits year in a date_format yyyy-MM partition column") {
    withTempDir { tempDir =>
      withTableName("optimizable_partition_expression") { table =>
        createTable(
          table,
          "c1 TIMESTAMP" :: Nil,
          "c2 STRING GENERATED ALWAYS AS (DATE_FORMAT(c1, 'yyyy-MM'))" :: Nil,
          Some(tempDir.getCanonicalPath))
        insertInto(
          tempDir.getCanonicalPath,
          Seq(Tuple1("12345-07-15 18:00:00"))
            .toDF("c1")
            .withColumn("c1", $"c1".cast(TimestampType))
        )

        checkAnswer(
          sql(s"select * from $table where c1 = CAST('12345-07-15 18:00:00' as timestamp)"),
          Row(new Timestamp(327420320400000L), "+12345-07"))
        withSQLConf(GENERATED_COLUMN_PARTITION_FILTER_OPTIMIZATION_ENABLED.key -> "false") {
          checkAnswer(
            sql(s"select * from $table where c1 = CAST('12345-07-15 18:00:00' as timestamp)"),
            Row(new Timestamp(327420320400000L), "+12345-07"))
        }
      }
    }
  }

  test("five digits year in a date_format yyyy-MM-dd-HH partition column") {
    withTempDir { tempDir =>
      withTableName("optimizable_partition_expression") { table =>
        createTable(
          table,
          "c1 TIMESTAMP" :: Nil,
          "c2 STRING GENERATED ALWAYS AS (DATE_FORMAT(c1, 'yyyy-MM-dd-HH'))" :: Nil,
          Some(tempDir.getCanonicalPath))
        insertInto(
          tempDir.getCanonicalPath,
          Seq(Tuple1("12345-07-15 18:00:00"))
            .toDF("c1")
            .withColumn("c1", $"c1".cast(TimestampType))
        )

        checkAnswer(
          sql(s"select * from $table where c1 = CAST('12345-07-15 18:00:00' as timestamp)"),
          Row(new Timestamp(327420320400000L), "+12345-07-15-18"))
        withSQLConf(GENERATED_COLUMN_PARTITION_FILTER_OPTIMIZATION_ENABLED.key -> "false") {
          checkAnswer(
            sql(s"select * from $table where c1 = CAST('12345-07-15 18:00:00' as timestamp)"),
            Row(new Timestamp(327420320400000L), "+12345-07-15-18"))
        }
      }
    }
  }

  test("end-to-end test of behaviors of write/read null on partition column") {
    //              unix_timestamp('12345-12', 'yyyy-MM') | unix_timestamp('+12345-12', 'yyyy-MM')
    //  EXCEPTION               fail                     |           327432240000
    //  CORRECTED               null                     |           327432240000
    //  LEGACY               327432240000                |               null
    withTempDir { tempDir =>
      withTableName("optimizable_partition_expression") { table =>
        createTable(
          table,
          "c1 TIMESTAMP" :: Nil,
          "c2 STRING GENERATED ALWAYS AS (DATE_FORMAT(c1, 'yyyy-MM'))" :: Nil,
          Some(tempDir.getCanonicalPath))

        // write in LEGACY
        withSQLConf(
        "spark.sql.legacy.timeParserPolicy" -> "CORRECTED"
        ) {
          insertInto(
            tempDir.getCanonicalPath,
            Seq(Tuple1("12345-07-01 00:00:00"))
              .toDF("c1")
              .withColumn("c1", $"c1".cast(TimestampType))
          )
          insertInto(
            tempDir.getCanonicalPath,
            Seq(Tuple1("+23456-07-20 18:30:00"))
              .toDF("c1")
              .withColumn("c1", $"c1".cast(TimestampType))
          )
        }

        // write in LEGACY
        withSQLConf(
          "spark.sql.legacy.timeParserPolicy" -> "LEGACY"
        ) {
          insertInto(
            tempDir.getCanonicalPath,
            Seq(Tuple1("+12349-07-01 00:00:00"))
              .toDF("c1")
              .withColumn("c1", $"c1".cast(TimestampType))
          )
          insertInto(
            tempDir.getCanonicalPath,
            Seq(Tuple1("+30000-12-30 20:00:00"))
              .toDF("c1")
              .withColumn("c1", $"c1".cast(TimestampType))
          )
        }

        // we have partitions based on CORRECTED + LEGACY parser (with +)
        assert(tempDir.listFiles().map(_.getName).toSet ==
          Set("c2=+23456-07", "c2=12349-07", "c2=30000-12", "c2=+12345-07", "_delta_log"))

        // read behaviors in CORRECTED, we still can query correctly
        withSQLConf("spark.sql.legacy.timeParserPolicy" -> "CORRECTED") {
          checkAnswer(
            sql(s"select (unix_timestamp('+20000-01', 'yyyy-MM')) as value"),
            Row(568971849600L)
          )
          checkAnswer(
            sql(s"select (unix_timestamp('20000-01', 'yyyy-MM')) as value"),
            Row(null)
          )
          checkAnswer(
            sql(s"select * from $table where " +
              s"c1 >= '20000-01-01 12:00:00'"),
            // 23456-07-20 18:30:00
            Row(new Timestamp(678050098200000L), "+23456-07") ::
              // 30000-12-30 20:00:00
              Row(new Timestamp(884572891200000L), "30000-12") :: Nil
          )
        }

        // read behaviors in LEGACY, we still can query correctly
        withSQLConf("spark.sql.legacy.timeParserPolicy" -> "LEGACY") {
          checkAnswer(
            sql(s"select (unix_timestamp('+20000-01', 'yyyy-MM')) as value"),
            Row(null)
          )
          checkAnswer(
            sql(s"select (unix_timestamp('20000-01', 'yyyy-MM')) as value"),
            Row(568971849600L)
          )
          checkAnswer(
            sql(s"select * from $table where " +
              s"c1 >= '20000-01-01 12:00:00'"),
            // 23456-07-20 18:30:00
            Row(new Timestamp(678050098200000L), "+23456-07") ::
              // 30000-12-30 20:00:00
              Row(new Timestamp(884572891200000L), "30000-12") :: Nil
          )
        }
      }
    }
  }
}