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

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import io.delta.kernel.client.TableClient
import io.delta.utils.RowSerDeUtils.serializeRowToJson
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

class DeltaScan(
  tableClient: TableClient,
  kernelScan: io.delta.kernel.Scan,
  readSchema: StructType) extends Scan with Batch {

  lazy val plannedInputPartitions: Array[InputPartition] =
    kernelScan.getScanFiles(tableClient).flatMap(_.getRows).map(row =>
      DeltaInputPartition(serializeRowToJson(row),
        serializeRowToJson(kernelScan.getScanState(tableClient)))).toArray

  override def readSchema(): StructType = readSchema

  override def toBatch: Batch = {
    this
  }

  // docs says should be called only once.. seems like it is called more than once
  // why is it called twice??
  override def planInputPartitions(): Array[InputPartition] = {
    // scalastyle:off println
    println("planning input partitions")
    println("num tasks: " + plannedInputPartitions.length)
    plannedInputPartitions
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new DeltaReaderFactory(readSchema)
  }

  // TODO override columnarSupportMode()?
}
