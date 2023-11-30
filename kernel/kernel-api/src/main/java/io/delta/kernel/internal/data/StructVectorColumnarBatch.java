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
package io.delta.kernel.internal.data;

import java.util.ArrayList;
import java.util.List;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * Wrapper to expose a struct type ColumnVector as a ColumnarBatch
 */
// TODO address the relation between struct CVs vs ColumnarBatches
// TODO should this be created somehow through the ExpressionHandler API?
public class StructVectorColumnarBatch implements ColumnarBatch {

    private final List<ColumnVector> columnVectors;
    private final int size;
    private final StructType schema;

    private StructVectorColumnarBatch(
        List<ColumnVector> vectors,
        int size,
        StructType schema) {
        this.columnVectors = vectors;
        this.size = size;
        this.schema = schema;
    }

    public StructVectorColumnarBatch(ColumnVector vector) {
        checkArgument(vector.getDataType() instanceof StructType);
        columnVectors = new ArrayList<>();
        schema = (StructType) vector.getDataType();
        for (int i = 0; i < schema.length(); i++) {
            columnVectors.add(vector.getChild(i));
        }
        size = vector.getSize();
    }

    @Override
    public StructType getSchema() {
        return schema;
    }

    @Override
    public ColumnVector getColumnVector(int ordinal) {
        return columnVectors.get(ordinal);
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public ColumnarBatch withNewColumn(
        int ordinal, StructField columnSchema, ColumnVector columnVector) {

        // Update the schema
        ArrayList<StructField> newStructFields = new ArrayList<>(schema.fields());
        newStructFields.ensureCapacity(schema.length() + 1);
        newStructFields.add(ordinal, columnSchema);
        StructType newSchema = new StructType(newStructFields);

        // Update the vectors
        ArrayList<ColumnVector> newColumnVectors = new ArrayList<>(columnVectors);
        newColumnVectors.ensureCapacity(columnVectors.size() + 1);
        newColumnVectors.add(ordinal, columnVector);

        return new StructVectorColumnarBatch(
            newColumnVectors, size, newSchema);
    }

    @Override
    public ColumnarBatch withDeletedColumnAt(int ordinal) {
        return ColumnarBatch.super.withDeletedColumnAt(ordinal);
    }
}
