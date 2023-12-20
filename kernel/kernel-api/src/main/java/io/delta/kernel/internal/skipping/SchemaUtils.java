package io.delta.kernel.internal.skipping;

import java.util.*;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.InternalSchemaUtils;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.*;

public class SchemaUtils {

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

    private static String[] prependArray(String[] names, String preElem) {
        String[] newNames = new String[names.length + 1];
        newNames[0] = preElem;
        System.arraycopy(names, 0, newNames, 1, names.length);
        return newNames;
    }


    private static Column getChildColumn(Column column, String parentName) {
        return new Column(prependArray(column.getNames(), parentName));
    }

    //////////////////////////////////////////////////////////////////////////////////
    // Stats schema utils
    //////////////////////////////////////////////////////////////////////////////////


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

    // Given a logical column and a stat type --> expression (path) to a physical stat column
    // Map of logical column to physical columns


    private static Tuple2<Map<Column, Column>, StructType> getMinMaxStatsSchema(
            StructType dataSchema) {

        List<StructField> fields = new ArrayList<>();
        Map<Column, Column> logicalToPhysical = new HashMap<>();

        for (StructField field : dataSchema.fields()) {
            if (isSkippingEligibleDataType(field.getDataType())) {
                fields.add(new StructField(
                    InternalSchemaUtils.getPhysicalName(field), field.getDataType(), true));
                logicalToPhysical.put(new Column(field.getName()),
                    new Column(InternalSchemaUtils.getPhysicalName(field)));
            } else if (field.getDataType() instanceof StructType) {
                Tuple2<Map<Column, Column>, StructType> childResult =
                    getMinMaxStatsSchema((StructType) field.getDataType());
                fields.add(
                    new StructField(InternalSchemaUtils.getPhysicalName(field),
                        childResult._2, true));
                for (Column key : childResult._1.keySet()) {
                    logicalToPhysical.put(
                        getChildColumn(key, field.getName()),
                        getChildColumn(childResult._1.get(key),
                            InternalSchemaUtils.getPhysicalName(field))
                    );
                }
            }
        }
        return new Tuple2<>(logicalToPhysical, new StructType(fields));
    }

}
