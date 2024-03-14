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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class DeltaReaderFactory(readSchema: StructType) extends PartitionReaderFactory {

  // For now do row-based reads until we can figure out how to do columnar with DV(selection vector)
  override def createReader(
    inputPartition: InputPartition): PartitionReader[InternalRow] = inputPartition match {
    case p: DeltaInputPartition =>
      new RowDataReader(p, readSchema)
    case _ =>
      throw new IllegalArgumentException("unknown input partition type")
  }

  override def createColumnarReader(
    inputPartition: InputPartition): PartitionReader[ColumnarBatch] = {
    // For now until we figure out BatchDataReader
    throw new UnsupportedOperationException("Columnar reads are not supported")
  }

  // False for now until we solve for DVs
  override def supportColumnarReads(inputPartition: InputPartition): Boolean = false
}
