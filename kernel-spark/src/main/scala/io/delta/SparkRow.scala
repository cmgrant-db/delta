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

import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class SparkRow(row: io.delta.kernel.data.Row)
  extends org.apache.spark.sql.catalyst.InternalRow {

  override def numFields: Int = row.getSchema().length()

  // TODO
  override def setNullAt(i: Int): Unit = {}

  // TODO
  override def update(i: Int, value: Any): Unit = {}

  // TODO
  override def copy(): InternalRow = new SparkRow(row)

  override def isNullAt(ordinal: Int): Boolean = row.isNullAt(ordinal)

  override def getBoolean(ordinal: Int): Boolean = row.getBoolean(ordinal)

  override def getByte(ordinal: Int): Byte = row.getByte(ordinal)

  override def getShort(ordinal: Int): Short = row.getShort(ordinal)

  override def getInt(ordinal: Int): Int = row.getInt(ordinal)

  override def getLong(ordinal: Int): Long = row.getLong(ordinal)

  override def getFloat(ordinal: Int): Float = row.getFloat(ordinal)

  override def getDouble(ordinal: Int): Double = row.getDouble(ordinal)

  // TODO
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
    throw unsupportedException

  // TODO
  override def getUTF8String(ordinal: Int): UTF8String = UTF8String.fromString(row.getString(ordinal))

  override def getBinary(ordinal: Int): Array[Byte] = row.getBinary(ordinal)

  override def getInterval(ordinal: Int): CalendarInterval = throw unsupportedException

  override def getStruct(ordinal: Int, numFields: Int): InternalRow =
    InternalRow(row.getStruct(ordinal))

  // TODO
  override def getArray(ordinal: Int): ArrayData = throw unsupportedException

  // TODO
  override def getMap(ordinal: Int): MapData = throw unsupportedException

  // TODO
  override def get(ordinal: Int, dataType: DataType): AnyRef = throw unsupportedException

  private def unsupportedException =
    new UnsupportedOperationException("SparkColumnarVector doesn't support this data type")
}
