/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.delta.kernel.client.ExpressionHandler;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.Tuple2;

public class PartitionUtils {
    private PartitionUtils() {}

    /**
     * Utility method to remove the given columns (as {@code columnsToRemove}) from the
     * given {@code physicalSchema}.
     *
     * @param physicalSchema
     * @param logicalSchema   To create a logical name to physical name map. Partition column names
     *                        are in logical space and we need to identify the equivalent
     *                        physical column name.
     * @param columnsToRemove
     * @return
     */
    public static StructType physicalSchemaWithoutPartitionColumns(
        StructType logicalSchema, StructType physicalSchema, Set<String> columnsToRemove) {
        if (columnsToRemove == null || columnsToRemove.size() == 0) {
            return physicalSchema;
        }

        // Partition columns are top-level only
        Map<String, String> physicalToLogical = new HashMap<String, String>() {
            {
                IntStream.range(0, logicalSchema.length())
                    .mapToObj(i -> new Tuple2<>(logicalSchema.at(i), physicalSchema.at(i)))
                    .forEach(tuple2 -> put(tuple2._2.getName(), tuple2._1.getName()));
            }
        };

        return new StructType(
            physicalSchema.fields().stream()
                .filter(field ->
                    !columnsToRemove.contains(physicalToLogical.get(field.getName())))
                .collect(Collectors.toList()));
    }

    public static ColumnarBatch withPartitionColumns(
        ExpressionHandler expressionHandler,
        ColumnarBatch dataBatch,
        StructType dataBatchSchema,
        Map<String, String> partitionValues,
        StructType schemaWithPartitionCols) {
        if (partitionValues == null || partitionValues.size() == 0) {
            // no partition column vectors to attach to.
            return dataBatch;
        }

        for (int colIdx = 0; colIdx < schemaWithPartitionCols.length(); colIdx++) {
            StructField structField = schemaWithPartitionCols.at(colIdx);

            if (partitionValues.containsKey(structField.getName())) {
                // Create a partition vector

                ExpressionEvaluator evaluator = expressionHandler.getEvaluator(
                    dataBatchSchema,
                    literalForPartitionValue(
                        structField.getDataType(),
                        partitionValues.get(structField.getName())),
                    structField.getDataType()
                );

                ColumnVector partitionVector = evaluator.eval(dataBatch);
                dataBatch = dataBatch.withNewColumn(colIdx, structField, partitionVector);
            }
        }

        return dataBatch;
    }

    private static Literal literalForPartitionValue(DataType dataType, String partitionValue) {
        if (partitionValue == null) {
            return Literal.ofNull(dataType);
        }

        if (dataType instanceof BooleanType) {
            return Literal.ofBoolean(Boolean.parseBoolean(partitionValue));
        }
        if (dataType instanceof ByteType) {
            return Literal.ofByte(Byte.parseByte(partitionValue));
        }
        if (dataType instanceof ShortType) {
            return Literal.ofShort(Short.parseShort(partitionValue));
        }
        if (dataType instanceof IntegerType) {
            return Literal.ofInt(Integer.parseInt(partitionValue));
        }
        if (dataType instanceof LongType) {
            return Literal.ofLong(Long.parseLong(partitionValue));
        }
        if (dataType instanceof FloatType) {
            return Literal.ofFloat(Float.parseFloat(partitionValue));
        }
        if (dataType instanceof DoubleType) {
            return Literal.ofDouble(Double.parseDouble(partitionValue));
        }
        if (dataType instanceof StringType) {
            return Literal.ofString(partitionValue);
        }
        if (dataType instanceof BinaryType) {
            return Literal.ofBinary(partitionValue.getBytes());
        }
        if (dataType instanceof DateType) {
            return Literal.ofDate(InternalUtils.daysSinceEpoch(Date.valueOf(partitionValue)));
        }
        if (dataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dataType;
            return Literal.ofDecimal(
                new BigDecimal(partitionValue), decimalType.getPrecision(), decimalType.getScale());
        }

        throw new UnsupportedOperationException("Unsupported partition column: " + dataType);
    }
}
