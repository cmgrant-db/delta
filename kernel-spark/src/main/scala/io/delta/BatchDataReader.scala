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

import java.util.Optional

import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.defaults.client.DefaultTableClient
import io.delta.kernel.utils.CloseableIterator
import io.delta.kernel.internal.InternalScanFileUtils
import io.delta.kernel.internal.data.ScanStateRow
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator

import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.hadoop.conf.Configuration

class BatchDataReader(inputPartition: DeltaInputPartition, readSchema: StructType)
  extends PartitionReader[ColumnarBatch] {

  // TODO use spark hadoop conf
  lazy val tableClient = DefaultTableClient.create(new Configuration())

  private def getDataIter = {
    val physicalDataIter: CloseableIterator[io.delta.kernel.data.ColumnarBatch] =
      tableClient.getParquetHandler()
        .readParquetFiles(
          singletonCloseableIterator(
            InternalScanFileUtils.getAddFileStatus(inputPartition.scanFileRow)),
          ScanStateRow.getPhysicalDataReadSchema(tableClient, inputPartition.scanFileRow),
          Optional.empty()
          /* optional predicate the connector can apply to filter data from the reader */
        )
    io.delta.kernel.Scan.transformPhysicalData(
      tableClient,
      inputPartition.scanState,
      inputPartition.scanFileRow,
      physicalDataIter
    )
  }

  lazy val iter: CloseableIterator[FilteredColumnarBatch] = getDataIter

  override def close(): Unit = {
    iter.close()
    closed = true
  }

  var curr: io.delta.kernel.data.FilteredColumnarBatch = null
  private var closed = false

  override def next(): Boolean = {
    if (!closed && iter.hasNext) {
      curr = iter.next()
      true
    } else {
      false
    }
  }

  override def get(): ColumnarBatch = {
    wrapColumnarBatch(curr)
  }

  // How to do batch reads and return a spark ColumnarBatch with the selection vector?
  // For now this is NOT taking into account DeletionVectors
  private def wrapColumnarBatch(
    batch: io.delta.kernel.data.FilteredColumnarBatch): ColumnarBatch = {

    val columnVectors: Array[org.apache.spark.sql.vectorized.ColumnVector] = {
      (0 until readSchema.length)
        .map(batch.getData.getColumnVector(_))
        .map(new SparkColumnVector(_))
        .toArray
    }
    new ColumnarBatch(columnVectors, batch.getData.getSize)
  }

}