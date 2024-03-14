/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.utils

import collection.JavaConverters._

import org.apache.spark.sql.types._

object SchemaUtils {

  def convertFromSparkSchema(schema: StructType): io.delta.kernel.types.StructType = {
    new io.delta.kernel.types.StructType(schema.map { field =>
      new io.delta.kernel.types.StructField(field.name,
        convertFromSparkDatatype(field.dataType), field.nullable)
    }.asJava)
  }

  def convertFromSparkDatatype(dataType: DataType): io.delta.kernel.types.DataType = {
    dataType match {
      case _: StringType => io.delta.kernel.types.StringType.STRING
      case _: BooleanType => io.delta.kernel.types.BooleanType.BOOLEAN
      case _: IntegerType => io.delta.kernel.types.IntegerType.INTEGER
      case _: LongType => io.delta.kernel.types.LongType.LONG
      case _ => // some primitive types for now
        throw new IllegalArgumentException("unsupported data type")
    }
  }

  def convertToSparkSchema(schema: io.delta.kernel.types.StructType): StructType = {
    StructType(schema.fields().asScala.map { field =>
      StructField(field.getName, convertToSparkDatatype(field.getDataType), field.isNullable)
    })
  }

  def convertToSparkDatatype(dataType: io.delta.kernel.types.DataType): DataType = {
    dataType match {
      case _: io.delta.kernel.types.StringType => StringType
      case _: io.delta.kernel.types.BooleanType => BooleanType
      case _: io.delta.kernel.types.IntegerType => IntegerType
      case _: io.delta.kernel.types.LongType => LongType
      case _ => // some primitive types for now
        throw new IllegalArgumentException("unsupported data type")
    }
  }
}
