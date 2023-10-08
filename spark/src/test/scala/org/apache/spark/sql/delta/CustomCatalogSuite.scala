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

import scala.collection.JavaConverters._

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

class CustomCatalogSuite extends QueryTest with SharedSparkSession
  with DeltaSQLCommandTest with DescribeDeltaDetailSuiteBase {

  override def sparkConf: SparkConf =
    super.sparkConf.set("spark.sql.catalog.dummy", classOf[DummyCatalog].getName)

  test("DESC DETAIL a delta table from DummyCatalog") {
    val tableName = "desc_detail_table"
    withTable(tableName) {
      val dummyCatalog =
        spark.sessionState.catalogManager.catalog("dummy").asInstanceOf[DummyCatalog]
      val tablePath = dummyCatalog.getTablePath(tableName)
      sql("SET CATALOG dummy")
      sql(f"CREATE TABLE $tableName (id bigint) USING delta")
      sql("SET CATALOG spark_catalog")
      // Insert some data into the table in the dummy catalog.
      // To make it simple, here we insert data directly into the table path.
      sql(f"INSERT INTO delta.`$tablePath` VALUES (0)")
      sql("SET CATALOG dummy")
      // Test simple desc detail command under the dummy catalog
      checkResult(
        sql(f"DESC DETAIL $tableName"),
        Seq("delta", 1),
        Seq("format", "numFiles"))
      // Test 3-part identifier
      checkResult(
        sql(f"DESC DETAIL dummy.default.$tableName"),
        Seq("delta", 1),
        Seq("format", "numFiles"))
      // Test table path
      checkResult(
        sql(f"DESC DETAIL delta.`$tablePath`"),
        Seq("delta", 1),
        Seq("format", "numFiles"))
      // Test 3-part identifier when the current catalog is not dummy catalog
      sql("SET CATALOG spark_catalog")
      checkResult(
        sql(f"DESC DETAIL dummy.default.$tableName"),
        Seq("delta", 1),
        Seq("format", "numFiles"))
    }
  }

  test("RESTORE a table from DummyCatalog") {
    val dummyCatalog =
      spark.sessionState.catalogManager.catalog("dummy").asInstanceOf[DummyCatalog]
    val tableName = "restore_table"
    val tablePath = dummyCatalog.getTablePath(tableName)
    withTable(tableName) {
      sql("SET CATALOG dummy")
      sql(f"CREATE TABLE $tableName (id bigint) USING delta")
      sql("SET CATALOG spark_catalog")
      // Insert some data into the table in the dummy catalog.
      // To make it simple, here we insert data directly into the table path.
      sql(f"INSERT INTO delta.`$tablePath` VALUES (0)")
      sql(f"INSERT INTO delta.`$tablePath` VALUES (1)")
      // Test 3-part identifier when the current catalog is the default catalog
      sql(f"RESTORE TABLE dummy.default.$tableName VERSION AS OF 1")
      checkAnswer(spark.table(f"dummy.default.$tableName"), spark.range(1).toDF())

      sql("SET CATALOG dummy")
      sql(f"RESTORE TABLE $tableName VERSION AS OF 0")
      checkAnswer(spark.table(tableName), Nil)
      sql(f"RESTORE TABLE $tableName VERSION AS OF 1")
      checkAnswer(spark.table(tableName), spark.range(1).toDF())
      // Test 3-part identifier
      sql(f"RESTORE TABLE dummy.default.$tableName VERSION AS OF 2")
      checkAnswer(spark.table(tableName), spark.range(2).toDF())
      // Test file path table
      sql(f"RESTORE TABLE delta.`$tablePath` VERSION AS OF 1")
      checkAnswer(spark.table(tableName), spark.range(1).toDF())
    }
  }

  test("Shallow Clone a table with time travel") {
    val srcTable = "shallow_clone_src_table"
    val destTable1 = "shallow_clone_dest_table_1"
    val destTable2 = "shallow_clone_dest_table_2"
    val destTable3 = "shallow_clone_dest_table_3"
    val destTable4 = "shallow_clone_dest_table_4"
    val dummyCatalog =
      spark.sessionState.catalogManager.catalog("dummy").asInstanceOf[DummyCatalog]
    val tablePath = dummyCatalog.getTablePath(srcTable)
    withTable(srcTable) {
      sql("SET CATALOG dummy")
      sql(f"CREATE TABLE $srcTable (id bigint) USING delta")
      sql("SET CATALOG spark_catalog")
      // Insert some data into the table in the dummy catalog.
      // To make it simple, here we insert data directly into the table path.
      sql(f"INSERT INTO delta.`$tablePath` VALUES (0)")
      sql(f"INSERT INTO delta.`$tablePath` VALUES (1)")
      withTable(destTable1) {
        // Test 3-part identifier when the current catalog is the default catalog
        sql(f"CREATE TABLE $destTable1 SHALLOW CLONE dummy.default.$srcTable VERSION AS OF 1")
        checkAnswer(spark.table(destTable1), spark.range(1).toDF())
      }

      sql("SET CATALOG dummy")
      Seq(true, false).foreach { createTableInDummy =>
        val (dest2, dest3, dest4) = if (createTableInDummy) {
          (destTable2, destTable3, destTable4)
        } else {
          val prefix = "spark_catalog.default"
          (s"$prefix.$destTable2", s"$prefix.$destTable3", s"$prefix.$destTable4")
        }
        withTable(dest2, dest3, dest4) {
          // Test simple shallow clone command under the dummy catalog
          sql(f"CREATE TABLE $dest2 SHALLOW CLONE $srcTable")
          checkAnswer(spark.table(dest2), spark.range(2).toDF())
          // Test time travel on the src table
          sql(f"CREATE TABLE $dest3 SHALLOW CLONE dummy.default.$srcTable VERSION AS OF 1")
          checkAnswer(spark.table(dest3), spark.range(1).toDF())
          // Test time travel on the src table delta path
          sql(f"CREATE TABLE $dest4 SHALLOW CLONE delta.`$tablePath` VERSION AS OF 1")
          checkAnswer(spark.table(dest4), spark.range(1).toDF())
        }
      }
    }
  }

  test("DESCRIBE HISTORY a delta table from DummyCatalog") {
    val tableName = "desc_history_table"
    withTable(tableName) {
      sql("SET CATALOG dummy")
      val dummyCatalog =
        spark.sessionState.catalogManager.catalog("dummy").asInstanceOf[DummyCatalog]
      val tablePath = dummyCatalog.getTablePath(tableName)
      sql(f"CREATE TABLE $tableName (column1 bigint) USING delta")
      sql("SET CATALOG spark_catalog")
      // Insert some data into the table in the dummy catalog.
      sql(f"INSERT INTO delta.`$tablePath` VALUES (0)")

      sql("SET CATALOG dummy")
      // Test simple desc detail command under the dummy catalog
      var result = sql(s"DESCRIBE HISTORY $tableName").collect()
      assert(result.length == 2)
      assert(result(0).getAs[Long]("version") == 1)
      // Test 3-part identifier
      result = sql(f"DESCRIBE HISTORY dummy.default.$tableName").collect()
      assert(result.length == 2)
      assert(result(0).getAs[Long]("version") == 1)
      // Test table path
      sql(f"DESC DETAIL delta.`$tablePath`").collect()
      assert(result.length == 2)
      assert(result(0).getAs[Long]("version") == 1)
      // Test 3-part identifier when the current catalog is not dummy catalog
      sql("SET CATALOG spark_catalog")
      result = sql(s"DESCRIBE HISTORY dummy.default.$tableName").collect()
      assert(result.length == 2)
      assert(result(0).getAs[Long]("version") == 1)
    }
  }
}

class DummyCatalog extends TableCatalog {
  private val spark: SparkSession = SparkSession.active
  private val tempDir: Path = new Path(Utils.createTempDir().getAbsolutePath)
  // scalastyle:off deltahadoopconfiguration
  private val fs: FileSystem =
    tempDir.getFileSystem(spark.sessionState.newHadoopConf())
  // scalastyle:on deltahadoopconfiguration

  override def name: String = "dummy"

  def getTablePath(tableName: String): Path = {
    new Path(tempDir.toString + "/" + tableName)
  }
  override def defaultNamespace(): Array[String] = Array("default")

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    val status = fs.listStatus(tempDir)
    status.filter(_.isDirectory).map { dir =>
      Identifier.of(namespace, dir.getPath.getName)
    }
  }

  override def tableExists(ident: Identifier): Boolean = {
    val tablePath = getTablePath(ident.name())
    fs.exists(tablePath)
  }
  override def loadTable(ident: Identifier): Table = {
    if (!tableExists(ident)) {
      throw new NoSuchTableException("")
    }
    val tablePath = getTablePath(ident.name())
    DeltaTableV2(spark = spark, path = tablePath, catalogTable = Some(createCatalogTable(ident)))
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val tablePath = getTablePath(ident.name())
    // Create an empty Delta table on the tablePath
    val part = partitions.map(_.arguments().head.toString)
    spark.createDataFrame(List.empty[Row].asJava, schema)
      .write.format("delta").partitionBy(part: _*).save(tablePath.toString)
    DeltaTableV2(spark = spark, path = tablePath, catalogTable = Some(createCatalogTable(ident)))
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("Alter table operation is not supported.")
  }

  override def dropTable(ident: Identifier): Boolean = {
    val tablePath = getTablePath(ident.name())
    try {
      fs.delete(tablePath, true)
      true
    } catch {
      case _: Exception => false
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Rename table operation is not supported.")
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // Initialize tempDir here
    if (!fs.exists(tempDir)) {
      fs.mkdirs(tempDir)
    }
  }

  private def createCatalogTable(ident: Identifier): CatalogTable = {
    val tablePath = getTablePath(ident.name())
    CatalogTable(
      identifier = TableIdentifier(ident.name(), defaultNamespace.headOption, Some(name)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(Some(tablePath.toUri), None, None, None, false, Map.empty),
      schema = spark.range(0).schema
    )
  }
}
