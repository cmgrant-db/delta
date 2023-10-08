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
package io.delta.kernel.internal;

import java.util.*;
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.Scan;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.*;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.types.TableSchemaSerDe;
import io.delta.kernel.internal.util.InternalSchemaUtils;
import io.delta.kernel.internal.util.PartitionUtils;
import static io.delta.kernel.internal.util.InternalUtils.checkArgument;
import static io.delta.kernel.internal.util.PartitionUtils.rewritePartitionPredicateOnScanFileSchema;

/**
 * Implementation of {@link Scan}
 */
public class ScanImpl
    implements Scan {
    /**
     * Schema of the snapshot from the Delta log being scanned in this scan. It is a logical schema
     * with metadata properties to derive the physical schema.
     */
    private final StructType snapshotSchema;

    private final Path dataPath;

    /**
     * Schema that we actually want to read.
     */
    private final StructType readSchema;
    private final CloseableIterator<FilteredColumnarBatch> filesIter;
    private final Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata;
    private final Lazy<Optional<Tuple2<Predicate, Predicate>>> partitionAndDataFilters;
    // Partition column names in lower case.
    private final Lazy<Set<String>> partitionColumnNames;

    private boolean accessedScanFiles;

    public ScanImpl(
        StructType snapshotSchema,
        StructType readSchema,
        Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata,
        CloseableIterator<FilteredColumnarBatch> filesIter,
        Optional<Predicate> filter,
        Path dataPath) {
        this.snapshotSchema = snapshotSchema;
        this.readSchema = readSchema;
        this.protocolAndMetadata = protocolAndMetadata;
        this.filesIter = filesIter;
        this.dataPath = dataPath;
        // Computing remaining filter requires access to metadata. We try to delay the metadata
        // loading as lazily as possible, that means remaining filter computation is also lazy.
        this.partitionAndDataFilters = new Lazy<>(() -> splitFilters(filter));
        this.partitionColumnNames = new Lazy<>(() -> loadPartitionColNames());
    }

    /**
     * Get an iterator of data files in this version of scan that survived the predicate pruning.
     *
     * @return data in {@link ColumnarBatch} batch format. Each row correspond to one survived file.
     */
    @Override
    public CloseableIterator<FilteredColumnarBatch> getScanFiles(TableClient tableClient) {
        if (accessedScanFiles) {
            throw new IllegalStateException("Scan files are already fetched from this instance");
        }
        accessedScanFiles = true;

        return applyPartitionPruning(tableClient, filesIter);
    }

    @Override
    public Row getScanState(TableClient tableClient) {
        return ScanStateRow.of(
            protocolAndMetadata.get()._2,
            protocolAndMetadata.get()._1,
            TableSchemaSerDe.toJson(readSchema),
            TableSchemaSerDe.toJson(
                InternalSchemaUtils.convertToPhysicalSchema(
                    readSchema,
                    snapshotSchema,
                    protocolAndMetadata.get()._2.getConfiguration()
                            .getOrDefault("delta.columnMapping.mode", "none")
                )
            ),
            dataPath.toUri().toString());
    }

    @Override
    public Optional<Predicate> getRemainingFilter() {
        return getDataFilters();
    }

    private Optional<Tuple2<Predicate, Predicate>> splitFilters(Optional<Predicate> filter) {
        return filter.map(predicate ->
            PartitionUtils.splitMetadataAndDataPredicates(predicate, partitionColumnNames.get()));
    }

    private Optional<Predicate> getDataFilters() {
        return removeAlwaysTrue(partitionAndDataFilters.get().map(filters -> filters._2));
    }

    private Optional<Predicate> getPartitionsFilters() {
        return removeAlwaysTrue(partitionAndDataFilters.get().map(filters -> filters._1));
    }

    /**
     * Consider `ALWAYS_TRUE` as no predicate.
     */
    private Optional<Predicate> removeAlwaysTrue(Optional<Predicate> predicate) {
        return predicate
            .filter(filter -> !filter.getName().equalsIgnoreCase("ALWAYS_TRUE"));
    }

    private CloseableIterator<FilteredColumnarBatch> applyPartitionPruning(
        TableClient tableClient,
        CloseableIterator<FilteredColumnarBatch> scanFileIter) {
        Optional<Predicate> partitionPredicate = getPartitionsFilters();
        if (!partitionPredicate.isPresent()) {
            // There is no partition filter, return the scan file iterator as is.
            return scanFileIter;
        }

        Metadata metadata = protocolAndMetadata.get()._2;
        Set<String> partitionColNames = partitionColumnNames.get();
        Map<String, DataType> partitionColNameToTypeMap = metadata.getSchema().fields().stream()
            .filter(field -> partitionColNames.contains(field.getName()))
            .collect(toMap(
                field -> field.getName().toLowerCase(Locale.ENGLISH),
                field -> field.getDataType()));

        Predicate predicateOnScanFileBatch = rewritePartitionPredicateOnScanFileSchema(
            partitionPredicate.get(),
            partitionColNameToTypeMap);

        PredicateEvaluator predicateEvaluator =
            tableClient.getExpressionHandler().getPredicateEvaluator(
                InternalScanFileUtils.SCAN_FILE_SCHEMA,
                predicateOnScanFileBatch);

        return filesIter.map(filteredScanFileBatch -> {
            ColumnVector newSelectionVector = predicateEvaluator.eval(
                filteredScanFileBatch.getData(),
                filteredScanFileBatch.getSelectionVector());
            return new FilteredColumnarBatch(
                filteredScanFileBatch.getData(),
                Optional.of(newSelectionVector));
        });
    }

    /**
     * Helper method to load the partition column names from the metadata.
     */
    private Set<String> loadPartitionColNames() {
        Metadata metadata = protocolAndMetadata.get()._2;
        ArrayValue partitionColValue = metadata.getPartitionColumns();
        ColumnVector partitionColNameVector = partitionColValue.getElements();
        Set<String> partitionColumnNames = new HashSet<>();
        for (int i = 0; i < partitionColValue.getSize(); i++) {
            checkArgument(!partitionColNameVector.isNullAt(i),
                "Expected a non-null partition column name");
            String partitionColName = partitionColNameVector.getString(i);
            checkArgument(partitionColName != null && !partitionColName.isEmpty(),
                "Expected non-null and non-empty partition column name");
            partitionColumnNames.add(partitionColName.toLowerCase(Locale.ENGLISH));
        }
        return Collections.unmodifiableSet(partitionColumnNames);
    }
}
