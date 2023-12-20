package io.delta.kernel.internal.skipping;

import java.util.*;
import java.util.stream.Collectors;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.*;
import static io.delta.kernel.internal.util.InternalSchemaUtils.getPhysicalName;


// TODO rename?
public class StatsFilterAndSchemaBuilder {

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

    private static String NUM_RECORDS = "numRecords";
    private static String MIN = "minValues";
    private static String MAX = "maxValues";
    private static String NULL_COUNT = "nullCount";

    private static boolean isSkippingEligibleDataType(DataType dataType) {
        return skippingEligibleTypeNames.contains(dataType.toString()) ||
            dataType instanceof DecimalType;
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



    // In order to get the Delta min/max stats schema from table schema, we do 1) replace field
    // name with physical name 2) set nullable to true 3) only keep stats eligible fields
    // 4) omits metadata in table schema as Delta stats schema does not need the metadata
    private StructType getMinMaxStatsSchema(StructType dataSchema) {
        List<StructField> fields = new ArrayList<>();
        for (StructField field : dataSchema.fields()) {
            if (isSkippingEligibleDataType(field.getDataType())) {
                fields.add(new StructField(
                    getPhysicalName(field), field.getDataType(), true));
            } else if (field.getDataType() instanceof StructType) {
                fields.add(
                    new StructField(getPhysicalName(field),
                        getMinMaxStatsSchema((StructType) field.getDataType()), true)
                );
            }
        }
        return new StructType(fields);
    }

    // In order to get the Delta null count schema from table schema, we do 1) replace field name
    // with physical name 2) set nullable to true 3) use LongType for all fields
    // 4) omits metadata in table schema as Delta stats schema does not need the metadata
    private StructType getNullCountSchema(StructType dataSchema) {
        List<StructField> fields = new ArrayList<>();
        for (StructField field : dataSchema.fields()) {
            if (field.getDataType() instanceof StructType) {
                fields.add(
                    new StructField(getPhysicalName(field),
                        getNullCountSchema((StructType) field.getDataType()), true)
                );
            } else {
                fields.add(new StructField(
                    getPhysicalName(field), LongType.LONG, true));
            }
        }
        return new StructType(fields);
    }

    private StructType getStatsSchema(StructType dataSchema) {
        StructType statsSchema = new StructType()
            .add(NUM_RECORDS, LongType.LONG, true);
        StructType minMaxStatsSchema = getMinMaxStatsSchema(dataSchema);
        if (minMaxStatsSchema.length() > 0) {
            statsSchema = statsSchema
                .add(MIN, getMinMaxStatsSchema(dataSchema), true)
                .add(MAX, getMinMaxStatsSchema(dataSchema), true);
        }
        StructType nullCountSchema = getNullCountSchema(dataSchema);
        if (nullCountSchema.length() > 0) {
            statsSchema = statsSchema
                .add(NULL_COUNT, getNullCountSchema(dataSchema), true);
        }
        return statsSchema;
    }

    private Map<Column, Tuple2<Column, DataType>> getLogicalToPhysicalColumnAndDataType(
            StructType dataSchema) {
        Map<Column, Tuple2<Column, DataType>> result = new HashMap<>();
        for (StructField field : dataSchema.fields()) {
            if (field.getDataType() instanceof StructType) {
                Map<Column, Tuple2<Column, DataType>> nestedCols =
                    getLogicalToPhysicalColumnAndDataType((StructType) field.getDataType());
                for (Column childLogicalCol : nestedCols.keySet()) {
                    Column childPhysicalCol = nestedCols.get(childLogicalCol)._1;
                    DataType childColDataType = nestedCols.get(childLogicalCol)._2;

                    result.put(
                        getChildColumn(childLogicalCol, field.getName()),
                        new Tuple2<>(
                            getChildColumn(childPhysicalCol, getPhysicalName(field)),
                            childColDataType
                        ));
                }
            } else {
                result.put(
                    new Column(field.getName()),
                    new Tuple2<>(
                        new Column(getPhysicalName(field)),
                        field.getDataType()
                    )
                );
            }
        }
        return result;
    }

    // TODO DOC
    private final StructType dataSchema;
    private final StructType fullPhysicalStatsSchema;
    private final Map<Column, Column> logicalToPhysicalColumn;
    private final Set<Column> availableMinMaxColumns;

    //  TODO what should be static and what should be instance
    public StatsFilterAndSchemaBuilder(StructType dataSchema) {
        this.dataSchema = dataSchema;
        this.fullPhysicalStatsSchema = getStatsSchema(dataSchema);
        Map<Column, Tuple2<Column, DataType>> logicalToPhysicalColumnAndDataType =
            getLogicalToPhysicalColumnAndDataType(dataSchema);
        this.logicalToPhysicalColumn = logicalToPhysicalColumnAndDataType.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue()._1 // map to just the column
                )
            );
        this.availableMinMaxColumns = logicalToPhysicalColumnAndDataType.entrySet().stream()
            .filter(e -> isSkippingEligibleDataType(e.getValue()._2))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }


    // should we check if the column is available before AND/OR after when making the predicate

    // check if its a skipping eligible column (null vs min/max) from logical
    // get stats columns (get physical column path from physical)


    // get pruned schema
    // get data filter

}
