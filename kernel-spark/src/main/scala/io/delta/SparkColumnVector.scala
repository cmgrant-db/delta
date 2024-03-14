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

import io.delta.utils.SchemaUtils
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarMap, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

class SparkColumnVector(kernelVector: io.delta.kernel.data.ColumnVector)
  extends org.apache.spark.sql.vectorized.ColumnVector(
    SchemaUtils.convertToSparkDatatype(kernelVector.getDataType)) {

  override def close(): Unit = kernelVector.close()

  // todo
  override def hasNull(): Boolean = true

  // todo
  override def numNulls(): Int = 0

  override def isNullAt(rowId: Int): Boolean = kernelVector.isNullAt(rowId)

  override def getBoolean(rowId: Int): Boolean = kernelVector.getBoolean(rowId)

  override def getByte(rowId: Int): Byte = kernelVector.getByte(rowId)

  override def getShort(rowId: Int): Short = kernelVector.getShort(rowId)

  override def getInt(rowId: Int): Int = kernelVector.getInt(rowId)

  override def getLong(rowId: Int): Long = kernelVector.getLong(rowId)

  override def getFloat(rowId: Int): Float = kernelVector.getFloat(rowId)

  override def getDouble(rowId: Int): Double = kernelVector.getDouble(rowId)

  override def getBinary(rowId: Int): Array[Byte] = kernelVector.getBinary(rowId)

  override def getUTF8String(rowId: Int): UTF8String =
    UTF8String.fromString(kernelVector.getString(rowId))

  private def unsupportedException =
    new UnsupportedOperationException("SparkColumnarVector doesn't support this data type")

  override def getArray(rowId: Int): ColumnarArray = throw unsupportedException
  override def getMap(rowId: Int): ColumnarMap = throw unsupportedException
  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    throw unsupportedException
  override def getChild(rowId: Int): ColumnVector = throw unsupportedException

}
