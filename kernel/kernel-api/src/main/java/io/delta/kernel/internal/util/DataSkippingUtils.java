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

import java.util.*;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.*;

public class DataSkippingUtils {

    private static Set<String> skippingEligibleTypeNames = new HashSet<String>() {
        {
            // TODO add("byte");
            add("short");
            add("integer");
            add("long");
            add("float");
            add("double");
            add("date");
            // TODO add("timestamp");
            add("string");
        }
    };

    private static boolean isSkippingEligibleDataType(DataType dataType) {
        return skippingEligibleTypeNames.contains(dataType.toString()) ||
            dataType instanceof DecimalType;
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
    private static boolean isSkippingEligibleColumn(Column column, StructType dataSchema) {
        // TODO check that is of AtomicType (not null, array, struct or map)
        return true;
    }

    private static boolean isSkippingEligibleLiteral(Literal literal) {
        return isSkippingEligibleDataType(literal.getDataType());
    }

    // In order to get the Delta min/max stats schema from table schema, we do 1) replace field
    // name with physical name 2) set nullable to true 3) only keep stats eligible fields
    // 4) omits metadata in table schema as Delta stats schema does not need the metadata
    private static StructType getMinMaxStatsSchema(StructType dataSchema) {
        // TODO support column mapping modes (replace with physical names)
        List<StructField> fields = new ArrayList<>();
        for (StructField field : dataSchema.fields()) {
            if (isSkippingEligibleDataType(field.getDataType())) {
                fields.add(new StructField(field.getName(), field.getDataType(), true));
            } else if (field.getDataType() instanceof StructType) {
                fields.add(
                    new StructField(field.getName(),
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
        // TODO support column mapping modes (replace with physical names)
        List<StructField> fields = new ArrayList<>();
        for (StructField field : dataSchema.fields()) {
            if (field.getDataType() instanceof StructType) {
                fields.add(
                    new StructField(field.getName(),
                        getNullCountSchema((StructType) field.getDataType()), true)
                );
            } else {
                fields.add(new StructField(field.getName(), LongType.LONG, true));
            }
        }
        return new StructType(fields);
    }

    public static StructType getStatsSchema(StructType dataSchema) {
        // TODO need to take metadata object instead for column mapping
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
        ColumnVector statsVector = scanFileBatch.getColumnVector(0).getChild(3);
        return tableClient.getJsonHandler().parseJson(statsVector, statsSchema, selectionVector);
    }

    private static Column getChildColumn(Column column, String parentName) {
        String[] newNames = new String[column.getNames().length + 1];
        newNames[0] = parentName;
        System.arraycopy(column.getNames(), 0, newNames, 1, column.getNames().length);
        return new Column(newNames);
    }

    private static Column getMinColumn(Column column) {
        return getChildColumn(column, "minValues");
    }

    private static Column getMaxColumn(Column column) {
        return getChildColumn(column, "maxValues");
    }

    // TODO should this take a predicate instead of Expression?
    // refactor this to be cleaner, can we use a builder? see spark traits
    public static Optional<DataSkippingPredicate> constructDataFilters(
            Expression dataFilter, StructType dataSchema) {

        if (dataFilter instanceof ScalarExpression) {
            switch (((ScalarExpression) dataFilter).getName().toUpperCase(Locale.ROOT))  {

                case "AND":
                    Optional<DataSkippingPredicate> e1Filter = constructDataFilters(
                        dataFilter.getChildren().get(0), dataSchema);
                    Optional<DataSkippingPredicate> e2Filter = constructDataFilters(
                        dataFilter.getChildren().get(1), dataSchema);
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
                        if (isSkippingEligibleColumn((Column) e1, dataSchema) &&
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
                            new ScalarExpression(
                                "=",
                                Arrays.asList(e2, e1)
                            ),
                            dataSchema
                        );
                    }

                // TODO more expressions
            }
        }
        return Optional.empty();
    }

    private static Column getColumn(StatsColumn col) {
        return getChildColumn(new Column(col.pathToColumn), col.statType);
    }

    // TODO formally add ISNOTNULL expression
    private static Predicate isNotNull(StatsColumn col) {
        return new Predicate("isNotNull", getColumn(col));
    }

    // this name is copied from DataSkippingReader but I find it confusing
    public static Predicate verifyStatsForFilter(Set<StatsColumn> referencedStats) {
        Set<StatsColumn> allStats = new HashSet<>();
        for (StatsColumn stat : referencedStats) {
            allStats.add(stat);
            if (stat.statType == "minValues" || stat.statType == "maxValues") {
                allStats.add(new StatsColumn("nullCount", stat.pathToColumn));
                allStats.add(new StatsColumn("numRecords", new String[0]));
            }
        }

        return allStats.stream().map(stat -> {
            if (stat.statType == "minValues" || stat.statType == "maxValues") {
                // A usable MIN or MAX stat must be non-NULL, unless the column is provably all-NULL
                //
                // NOTE: We don't care about NULL/missing NULL_COUNT and NUM_RECORDS here, because
                // the separate NULL checks we emit for those columns will force the overall
                // validation predicate conjunction to FALSE in that case -- AND(FALSE, <anything>)
                // is FALSE.
                Predicate e1 = isNotNull(stat);
                Predicate e2 = new Predicate(
                    "=",
                    getColumn(new StatsColumn("numRecords", new String[0])),
                    getColumn(new StatsColumn("nullCount", stat.pathToColumn))
                );
                return new Predicate("OR", e1, e2);
            } else {
                // Other stats, such as NULL_COUNT and NUM_RECORDS stat, merely need to be non-NULL
                return isNotNull(stat);
            }
        }).reduce(
            AlwaysTrue.ALWAYS_TRUE,
            (e1, e2) -> new And(e1, e2)
        );
    }
}
