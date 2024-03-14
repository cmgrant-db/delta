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

package io.delta;

import collection.JavaConverters._
import io.delta.utils.SchemaUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.delta.kernel.defaults.client.DefaultTableClient

case class DeltaTable(path: Path) extends Table with SupportsRead {

  lazy val tableClient = new DefaultTableClient()
  lazy val deltaTable = io.delta.kernel.Table.forPath(tableClient, path.toString)
  lazy val snapshot = deltaTable.getLatestSnapshot(tableClient)

  override def name(): String = {
    s"delta.`${path.toString}`"
  }

  // TODO deprecated, override columns instead
  override def schema(): StructType = {
    SchemaUtils.convertToSparkSchema(snapshot.getSchema(tableClient))
  }

  // TODO do we need to return the real partitioning here?
  // override def partitioning(): Array[Transform]

  // TODO override properties()?

  override def capabilities(): java.util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new DeltaScanBuilder(tableClient, snapshot)
  }
}