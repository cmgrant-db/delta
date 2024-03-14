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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class DeltaDataSource extends TableProvider with DataSourceRegister {

  override def shortName(): String = "delta"

  def inferSchema: StructType = new StructType() // empty

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = inferSchema
  // TODO do we need to infer the schema correctly here?
  // Delta-Spark uses an empty schema
  // Iceberg returns null
  // If we want this can call getTable().schema()

  // TODO inferPartitioning?

  override def getTable(schema: StructType, partitioning: Array[Transform],
    properties: java.util.Map[String, String]): Table = {
    // When should we fix the snapshot? If we delay inferring the schema/partitioning we can wait
    // until later?
    val options = new CaseInsensitiveStringMap(properties)
    val path = options.get("path")
    if (path == null) throw new IllegalArgumentException("Table path is not specified.")
    DeltaTable(new Path(path))
  }
}
