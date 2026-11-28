/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.azuretable;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Bounded source for Azure Table Storage entities.
 *
 * <p>Discovered tables share one schema that includes the table name and exact entity keys.
 */
public final class AzureTableSource
        implements SeaTunnelSource<SeaTunnelRow, AzureTableSplit, AzureTableSplit> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "AzureTableStorage";

    private final AzureTableConfig config;

    public AzureTableSource(ReadonlyConfig options) {
        config = new AzureTableConfig(options);
    }

    @Override
    public void setJobContext(JobContext context) {
        if (JobMode.STREAMING.equals(context.getJobMode())) {
            throw new IllegalArgumentException("AzureTable requires BATCH mode");
        }
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "table_name", BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "partition_key",
                                        BasicType.STRING_TYPE,
                                        0,
                                        false,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "row_key", BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "timestamp", BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "etag", BasicType.STRING_TYPE, 0, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "record", BasicType.STRING_TYPE, 0, false, null, null))
                        .build();
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, TablePath.DEFAULT),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null));
    }

    @Override
    public SourceReader<SeaTunnelRow, AzureTableSplit> createReader(SourceReader.Context context) {
        return new AzureTableReader(config, context);
    }

    /**
     * Creates the discovery or explicit-table position before the first request.
     *
     * <p>The coordinator retains the pending split for checkpoints before assignment.
     */
    @Override
    public SourceSplitEnumerator<AzureTableSplit, AzureTableSplit> createEnumerator(
            SourceSplitEnumerator.Context<AzureTableSplit> context) {
        return new Enumerator(context, config, config.newSplit());
    }

    /**
     * Restores the pending tenant scan under the same account, endpoint and table selection.
     *
     * <p>The engine can independently return a newer reader position before assignment.
     */
    @Override
    public SourceSplitEnumerator<AzureTableSplit, AzureTableSplit> restoreEnumerator(
            SourceSplitEnumerator.Context<AzureTableSplit> context, AzureTableSplit checkpoint) {
        checkpoint.validate(config);
        return new Enumerator(context, config, checkpoint);
    }

    /**
     * Keeps the initial scan available before reader assignment and returns scope on reassignment.
     *
     * <p>This covers a coordinator checkpoint taken before the active subtask receives its split.
     */
    static final class Enumerator
            implements SourceSplitEnumerator<AzureTableSplit, AzureTableSplit> {
        private final Context<AzureTableSplit> context;
        private final AzureTableConfig config;
        private final Set<Integer> inactive = new HashSet<>();
        private AzureTableSplit latest;
        private boolean running;
        private boolean assigned;

        Enumerator(
                Context<AzureTableSplit> context,
                AzureTableConfig config,
                AzureTableSplit starting) {
            this.context = context;
            this.config = config;
            latest = starting.copy();
        }

        @Override
        public void open() {}

        /**
         * Assigns the scan after the engine registers its source subtasks.
         *
         * <p>Subtasks beyond index zero finish without creating a provider request worker.
         */
        @Override
        public void run() {
            synchronized (context) {
                running = true;
                context.registeredReaders().forEach(this::dispatch);
            }
        }

        private void dispatch(int reader) {
            if (!running) {
                return;
            }
            if (reader == 0 && !assigned) {
                context.assignSplit(0, latest.copy());
                assigned = true;
            } else if (reader != 0 && inactive.add(reader)) {
                context.signalNoMoreSplits(reader);
            }
        }

        @Override
        public int currentUnassignedSplitSize() {
            synchronized (context) {
                return assigned ? 0 : 1;
            }
        }

        /**
         * Replaces the initial fallback with the active reader's recovered progress.
         *
         * <p>The replacement is also safe when the engine returns the split after registration.
         */
        @Override
        public void addSplitsBack(List<AzureTableSplit> splits, int subtask) {
            synchronized (context) {
                if (splits.isEmpty()) {
                    return;
                }
                if (subtask != 0 || splits.size() != 1) {
                    throw new IllegalStateException("Invalid AzureTable splits returned by reader");
                }
                AzureTableSplit recovered = splits.get(0);
                recovered.validate(config);
                latest = recovered.copy();
                assigned = false;
                if (context.registeredReaders().contains(0)) {
                    dispatch(0);
                }
            }
        }

        @Override
        public void registerReader(int subtask) {
            synchronized (context) {
                dispatch(subtask);
            }
        }

        @Override
        public void handleSplitRequest(int subtask) {
            registerReader(subtask);
        }

        /**
         * Captures an independent coordinator fallback for the tenant scan.
         *
         * <p>Rows emitted after assignment are checkpointed by the active reader.
         */
        @Override
        public AzureTableSplit snapshotState(long checkpointId) {
            synchronized (context) {
                return latest.copy();
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void close() {}
    }
}
