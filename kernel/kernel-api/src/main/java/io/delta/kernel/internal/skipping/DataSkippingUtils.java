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
package io.delta.kernel.internal.skipping;

import java.util.*;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.*;
import io.delta.kernel.internal.util.InternalSchemaUtils;
import io.delta.kernel.types.*;

public class DataSkippingUtils {

    //////////////////////////////////////////////////////////////////////////////////
    // Skipping eligibility utils
    //////////////////////////////////////////////////////////////////////////////////

    private static Set<String> skippingEligibleTypeNames = new HashSet<String>() {
        {
            add("byte");
            add("short");
            add("integer");
            add("long");
            add("float");
            add("double");
            add("date");
            add("timestamp");
            add("string");
        }
    };

    private static boolean isSkippingEligibleDataType(DataType dataType) {
        return skippingEligibleTypeNames.contains(dataType.toString()) ||
            dataType instanceof DecimalType;
    }

    private static Map<Column, DataType> getLeafColumns(StructType dataSchema) {
        Map<Column, DataType> result = new HashMap<>();
        for (StructField field : dataSchema.fields()) {
            if (field.getDataType() instanceof StructType) {
                Map<Column, DataType> nestedColumns = getLeafColumns(
                    (StructType) field.getDataType());
                for (Column nestedCol : nestedColumns.keySet()) {
                    result.put(
                        new Column(prependArray(nestedCol.getNames(), field.getName())),
                        nestedColumns.get(nestedCol)
                    );
                }
            } else {
                result.put(new Column(field.getName()), field.getDataType());
            }
        }
        return result;
    }

    /**
     * An extractor that matches on access of a skipping-eligible column. We only collect stats for
     * leaf columns, so internal columns of nested types are ineligible for skipping.
     *
     * NOTE: This check is sufficient for safe use of NULL_COUNT stats, but safe use of MIN and MAX
     * stats requires additional restrictions on column data type (see SkippingEligibleLiteral).
     *
     * @return The path to the column and the column's data type if it exists and is eligible.
     *         Otherwise, return None.
     */
    private static boolean isSkippingEligibleColumn(
            Column column, Map<Column, DataType> leafColumnsToDataTypes) {
        if (!leafColumnsToDataTypes.containsKey(column)) {
            // double check that the expected behavior is non-existent columns just don't filter not
            // error
            return false;
        }
        DataType dataType = leafColumnsToDataTypes.get(column);
        return !(dataType instanceof MapType || dataType instanceof ArrayType);
    }

    private static boolean isSkippingEligibleLiteral(Literal literal) {
        return isSkippingEligibleDataType(literal.getDataType());
    }

    //////////////////////////////////////////////////////////////////////////////////
    // Stats schema utils
    //////////////////////////////////////////////////////////////////////////////////

    // TODO tests with column mapping
    // In order to get the Delta min/max stats schema from table schema, we do 1) replace field
    // name with physical name 2) set nullable to true 3) only keep stats eligible fields
    // 4) omits metadata in table schema as Delta stats schema does not need the metadata
    private static StructType getMinMaxStatsSchema(StructType dataSchema) {
        List<StructField> fields = new ArrayList<>();
        for (StructField field : dataSchema.fields()) {
            if (isSkippingEligibleDataType(field.getDataType())) {
                fields.add(new StructField(
                    InternalSchemaUtils.getPhysicalName(field), field.getDataType(), true));
            } else if (field.getDataType() instanceof StructType) {
                fields.add(
                    new StructField(InternalSchemaUtils.getPhysicalName(field),
                        getMinMaxStatsSchema((StructType) field.getDataType()), true)
                );
            }
        }
        return new StructType(fields);
    }

    // In order to get the Delta null count schema from table schema, we do 1) replace field name
    // with physical name 2) set nullable to true 3) use LongType for all fields
    // 4) omits metadata in table schema as Delta stats schema does not need the metadata
    private static StructType getNullCountSchema(StructType dataSchema) {
        List<StructField> fields = new ArrayList<>();
        for (StructField field : dataSchema.fields()) {
            if (field.getDataType() instanceof StructType) {
                fields.add(
                    new StructField(InternalSchemaUtils.getPhysicalName(field),
                        getNullCountSchema((StructType) field.getDataType()), true)
                );
            } else {
                fields.add(new StructField(
                    InternalSchemaUtils.getPhysicalName(field), LongType.LONG, true));
            }
        }
        return new StructType(fields);
    }

    public static StructType getStatsSchema(StructType dataSchema) {
        // put stats field names into variables
        StructType statsSchema = new StructType()
            .add("numRecords", LongType.LONG, true);
        StructType minMaxStatsSchema = getMinMaxStatsSchema(dataSchema);
        if (minMaxStatsSchema.length() > 0) {
            statsSchema = statsSchema
                .add("minValues", getMinMaxStatsSchema(dataSchema), true)
                .add("maxValues", getMinMaxStatsSchema(dataSchema), true);
        }
        StructType nullCountSchema = getNullCountSchema(dataSchema);
        if (nullCountSchema.length() > 0) {
            statsSchema = statsSchema
                .add("nullCount", getNullCountSchema(dataSchema), true);
        }
        return statsSchema;
    }

    public static ColumnarBatch parseJsonStats(
        TableClient tableClient, ColumnarBatch scanFileBatch, StructType statsSchema,
        Optional<ColumnVector> selectionVector) {
        // indices shouldn't be hardcoded here
        ColumnVector statsVector = scanFileBatch.getColumnVector(0).getChild(6);
        return tableClient.getJsonHandler().parseJson(statsVector, statsSchema, selectionVector);
    }

    private static String[] prependArray(String[] names, String preElem) {
        String[] newNames = new String[names.length + 1];
        newNames[0] = preElem;
        System.arraycopy(names, 0, newNames, 1, names.length);
        return newNames;
    }

    private static Column getChildColumn(Column column, String parentName) {
        return new Column(prependArray(column.getNames(), parentName));
    }

    private static Column getMinColumn(Column column) {
        return getChildColumn(column, "minValues");
    }

    private static Column getMaxColumn(Column column) {
        return getChildColumn(column, "maxValues");
    }

    // TODO transform for physical names???!!!
    // getStatsColumnOpt!!!! need to deal with physical schema
    // this happens in the builder through StatsProvider!

    // refactor this to be cleaner, can we use a builder? see spark traits

    // Get the data filters AND stats schema (pruned based on the data filters)
    public static Optional<DataSkippingPredicate> constructDataFilters(
            Predicate dataFilter, StructType dataSchema) {



        // we need to decide if we can use a column (need data schema)
        // we need to generate the skipping predicate given a column (logical name)
        //   - transform to physical name
        //   - create with stats path names
        //   - how should this be structured?

        // Create object like SkippingEligibleChecker?
        Map<Column, DataType> leafColumnsToDataTypes = getLeafColumns(dataSchema);

        switch (dataFilter.getName().toUpperCase(Locale.ROOT))  {

            case "AND":
                Optional<DataSkippingPredicate> e1Filter = constructDataFilters(
                    (Predicate) dataFilter.getChildren().get(0), dataSchema);
                Optional<DataSkippingPredicate> e2Filter = constructDataFilters(
                    (Predicate) dataFilter.getChildren().get(1), dataSchema);
                if (e1Filter.isPresent() && e2Filter.isPresent()) {
                    return Optional.of(
                        new DataSkippingPredicate(
                            new And(e1Filter.get().predicate, e2Filter.get().predicate),
                            new HashSet() {
                                {
                                    addAll(e1Filter.get().referencedStats);
                                    addAll(e2Filter.get().referencedStats);
                                }
                            }
                        ));
                } else if (e1Filter.isPresent()) {
                    return e1Filter;
                } else {
                    return e2Filter; // possibly none
                }

            case "=":
                Expression e1 = dataFilter.getChildren().get(0);
                Expression e2 = dataFilter.getChildren().get(1);

                if (e1 instanceof Column && e2 instanceof Literal) {
                    if (isSkippingEligibleColumn((Column) e1, leafColumnsToDataTypes) &&
                        isSkippingEligibleLiteral((Literal) e2)) {

                        Predicate left = new Predicate("<=", getMinColumn((Column) e1), e2);
                        Predicate right = new Predicate("<=", e2, getMaxColumn((Column) e1));
                        return Optional.of(new DataSkippingPredicate(
                            new And(left, right),
                            new HashSet<StatsColumn>() {
                                {
                                    add(new StatsColumn("minValues", ((Column) e1).getNames()));
                                    add(new StatsColumn("maxValues", ((Column) e1).getNames()));
                                }
                            }));
                    }
                } else if (e2 instanceof Column && e1 instanceof Literal) {
                    return constructDataFilters(
                        new Predicate("=", e2, e1),
                        dataSchema
                    );
                }

            // TODO more expressions

            default:
                return Optional.empty();
        }
    }

    private static Optional<Column> getStatsColumnOpt(String statType, Column column) {
        // check that the stat type exists?
        // transform it??
    }
}
