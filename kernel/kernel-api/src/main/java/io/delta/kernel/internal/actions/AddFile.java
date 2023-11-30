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
package io.delta.kernel.internal.actions;

import io.delta.kernel.types.*;

import io.delta.kernel.internal.util.DataSkippingUtils;

/**
 * Delta log action representing an `AddFile`
 */
public class AddFile {
    // TODO: further restrict the fields to read from AddFile depending upon
    // the whether stats are needed or not: https://github.com/delta-io/delta/issues/1961

    // TODO consider how we treat and expose field ordinals:
    //   - is there a better way to expose a consistent schema that also works with stats_parsed
    //   - can we add a map fieldName --> ordinal and use testing to enforce it?
    //   - seems like we duplicate these ordianals/schemas in a few places, how can we better
    //     organize it

    public static StructType getAddFileSchema(StructType dataSchema) {
        return new StructType()
            .add("path", StringType.STRING, false /* nullable */)
            .add("partitionValues",
                new MapType(StringType.STRING, StringType.STRING, true),
                false /* nullable*/)
            .add("size", LongType.LONG, false /* nullable*/)
            .add("stats", StringType.STRING, true /* nullable */)
            .add("modificationTime", LongType.LONG, false /* nullable*/)
            .add("dataChange", BooleanType.BOOLEAN, false /* nullable*/)
            .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA, true /* nullable */)
            .add("stats_parsed",
                DataSkippingUtils.getStatsSchema(dataSchema), true /* nullable */);
    }
}
