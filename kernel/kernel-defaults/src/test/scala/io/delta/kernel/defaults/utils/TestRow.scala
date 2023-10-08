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
package io.delta.kernel.defaults.utils

import scala.collection.JavaConverters._

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue, Row}
import io.delta.kernel.types._

/**
 * Corresponding Scala class for each Kernel data type:
 * - BooleanType --> boolean
 * - ByteType --> byte
 * - ShortType --> short
 * - IntegerType --> int
 * - LongType --> long
 * - FloatType --> float
 * - DoubleType --> double
 * - StringType --> String
 * - DateType --> int (number of days since the epoch)
 * - TimestampType --> long (number of microseconds since the unix epoch)
 * - DecimalType --> java.math.BigDecimal
 * - BinaryType --> Array[Byte]
 * - ArrayType --> Seq[Any]
 * - MapType --> Map[Any, Any]
 * - StructType --> TestRow
 *
 * For complex types array and map, the inner elements types should align with this mapping.
 */
class TestRow(val values: Array[Any]) {

  def length: Int = values.length

  def get(i: Int): Any = values(i)

  def toSeq: Seq[Any] = values.clone()

  def mkString(start: String, sep: String, end: String): String = {
    val n = length
    val builder = new StringBuilder
    builder.append(start)
    if (n > 0) {
      builder.append(get(0))
      var i = 1
      while (i < n) {
        builder.append(sep)
        builder.append(get(i))
        i += 1
      }
    }
    builder.append(end)
    builder.toString()
  }

  override def toString: String = this.mkString("[", ",", "]")
}

object TestRow {

  /**
   * Construct a [[TestRow]] with the given values. See the docs for [[TestRow]] for
   * the scala type corresponding to each Kernel data type.
   */
  def apply(values: Any*): TestRow = {
    new TestRow(values.toArray)
  }

  /**
   * Construct a [[TestRow]] with the same values as a Kernel [[Row]].
   */
  def apply(row: Row): TestRow = {
    TestRow.fromSeq(row.getSchema.fields().asScala.zipWithIndex.map { case (field, i) =>
      field.getDataType match {
        case _ if row.isNullAt(i) => null
        case _: BooleanType => row.getBoolean(i)
        case _: ByteType => row.getByte(i)
        case _: IntegerType => row.getInt(i)
        case _: LongType => row.getLong(i)
        case _: ShortType => row.getShort(i)
        case _: DateType => row.getInt(i)
        case _: TimestampType => row.getLong(i)
        case _: FloatType => row.getFloat(i)
        case _: DoubleType => row.getDouble(i)
        case _: StringType => row.getString(i)
        case _: BinaryType => row.getBinary(i)
        case _: DecimalType => row.getDecimal(i)
        case _: ArrayType => arrayValueToScalaSeq(row.getArray(i))
        case _: MapType => mapValueToScalaMap(row.getMap(i))
        case _: StructType => TestRow(row.getStruct(i))
        case _ => throw new UnsupportedOperationException("unrecognized data type")
      }
    })
  }

  /**
   * Retrieves the value at `rowId` in the column vector as it's corresponding scala type.
   * See the [[TestRow]] docs for details.
   */
  private def getAsTestObject(vector: ColumnVector, rowId: Int): Any = {
    vector.getDataType match {
      case _ if vector.isNullAt(rowId) => null
      case _: BooleanType => vector.getBoolean(rowId)
      case _: ByteType => vector.getByte(rowId)
      case _: IntegerType => vector.getInt(rowId)
      case _: LongType => vector.getLong(rowId)
      case _: ShortType => vector.getShort(rowId)
      case _: DateType => vector.getInt(rowId)
      case _: TimestampType => vector.getLong(rowId)
      case _: FloatType => vector.getFloat(rowId)
      case _: DoubleType => vector.getDouble(rowId)
      case _: StringType => vector.getString(rowId)
      case _: BinaryType => vector.getBinary(rowId)
      case _: DecimalType => vector.getDecimal(rowId)
      case _: ArrayType => arrayValueToScalaSeq(vector.getArray(rowId))
      case _: MapType => mapValueToScalaMap(vector.getMap(rowId))
       case _: StructType => TestRow(vector.getStruct(rowId))
      case _ => throw new UnsupportedOperationException("unrecognized data type")
    }
  }

  private def arrayValueToScalaSeq(arrayValue: ArrayValue): Seq[Any] = {
    val elemVector = arrayValue.getElements
    (0 until arrayValue.getSize).map { i =>
      getAsTestObject(elemVector, i)
    }
  }

  private def mapValueToScalaMap(mapValue: MapValue): Map[Any, Any] = {
    val keyVector = mapValue.getKeys()
    val valueVector = mapValue.getValues()
    (0 until mapValue.getSize).map { i =>
      getAsTestObject(keyVector, i) -> getAsTestObject(valueVector, i)
    }.toMap
  }

  /**
   * Construct a [[TestRow]] from the given seq of values. See the docs for [[TestRow]] for
   * the scala type corresponding to each Kernel data type.
   */
  def fromSeq(values: Seq[Any]): TestRow = {
    new TestRow(values.toArray)
  }

  /**
   * Construct a [[TestRow]] with the elements of the given tuple. See the docs for
   * [[TestRow]] for the scala type corresponding to each Kernel data type.
   */
  def fromTuple(tuple: Product): TestRow = fromSeq(tuple.productIterator.toSeq)
}
