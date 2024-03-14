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
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.InternalRow

class RowDataReader(inputPartition: DeltaInputPartition, readSchema: StructType)
  extends PartitionReader[InternalRow] {

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

  lazy val batchIter: CloseableIterator[FilteredColumnarBatch] = getDataIter
  var rowIter: CloseableIterator[io.delta.kernel.data.Row] = null
  var curr: io.delta.kernel.data.Row = null
  private var closed = false

  override def close(): Unit = {
    batchIter.close()
    if (rowIter != null) {
      rowIter.close()
    }
    closed = true
  }

  // TODO double check this?
  override def next(): Boolean = {
    if (!closed) {
      if (rowIter != null && rowIter.hasNext) {
        curr = rowIter.next()
        true
      } else if (batchIter.hasNext) {
        rowIter = batchIter.next().getRows
        next()
      } else {
        false
      }
    } else {
      false
    }
  }

  override def get(): InternalRow = {
    new SparkRow(curr)
  }
}
