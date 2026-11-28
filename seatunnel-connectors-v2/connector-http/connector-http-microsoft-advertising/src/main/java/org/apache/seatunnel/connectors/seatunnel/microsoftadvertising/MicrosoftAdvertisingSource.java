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
package org.apache.seatunnel.connectors.seatunnel.microsoftadvertising;

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

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Bounded source for Microsoft Advertising entities, reports and bulk data.
 *
 * <p>The row envelope carries a Microsoft Advertising account and its owning customer.
 */
public final class MicrosoftAdvertisingSource
        implements SeaTunnelSource<
                SeaTunnelRow, MicrosoftAdvertisingSplit, MicrosoftAdvertisingSplit> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "MicrosoftAdvertising";

    private final MicrosoftAdvertisingConfig config;

    public MicrosoftAdvertisingSource(ReadonlyConfig options) {
        config = new MicrosoftAdvertisingConfig(options);
    }

    @Override
    public void setJobContext(JobContext context) {
        if (JobMode.STREAMING.equals(context.getJobMode())) {
            throw new IllegalArgumentException("MicrosoftAdvertising requires BATCH mode");
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
                        .column(stringColumn("resource", false))
                        .column(stringColumn("account_id", false))
                        .column(stringColumn("customer_id", false))
                        .column(stringColumn("record_key", true))
                        .column(stringColumn("record", false))
                        .build();
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, TablePath.DEFAULT),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null));
    }

    private static PhysicalColumn stringColumn(String name, boolean nullable) {
        return PhysicalColumn.of(name, BasicType.STRING_TYPE, 0, nullable, null, null);
    }

    @Override
    public SourceReader<SeaTunnelRow, MicrosoftAdvertisingSplit> createReader(
            SourceReader.Context context) {
        return new MicrosoftAdvertisingReader(config, context);
    }

    /**
     * Freezes the reporting window before the first provider request.
     *
     * <p>The coordinator retains the pending split for checkpoints before assignment.
     */
    @Override
    public SourceSplitEnumerator<MicrosoftAdvertisingSplit, MicrosoftAdvertisingSplit>
            createEnumerator(SourceSplitEnumerator.Context<MicrosoftAdvertisingSplit> context) {
        return new Enumerator(context, config, config.newSplit(Instant.now()));
    }

    /**
     * Restores the pending advertising scan under the same frozen dates, resource selections and
     * account filters.
     *
     * <p>The engine can independently return a newer reader position before assignment.
     */
    @Override
    public SourceSplitEnumerator<MicrosoftAdvertisingSplit, MicrosoftAdvertisingSplit>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<MicrosoftAdvertisingSplit> context,
                    MicrosoftAdvertisingSplit checkpoint) {
        checkpoint.validate(config);
        return new Enumerator(context, config, checkpoint);
    }

    /**
     * Keeps the initial scan available before reader assignment and returns scope on reassignment.
     *
     * <p>This covers a coordinator checkpoint taken before the active subtask receives its split.
     */
    static final class Enumerator
            implements SourceSplitEnumerator<MicrosoftAdvertisingSplit, MicrosoftAdvertisingSplit> {
        private final Context<MicrosoftAdvertisingSplit> context;
        private final MicrosoftAdvertisingConfig config;
        private final Set<Integer> inactive = new HashSet<>();
        private MicrosoftAdvertisingSplit latest;
        private boolean running;
        private boolean assigned;

        Enumerator(
                Context<MicrosoftAdvertisingSplit> context,
                MicrosoftAdvertisingConfig config,
                MicrosoftAdvertisingSplit starting) {
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
        public void addSplitsBack(List<MicrosoftAdvertisingSplit> splits, int subtask) {
            synchronized (context) {
                if (splits.isEmpty()) {
                    return;
                }
                if (subtask != 0 || splits.size() != 1) {
                    throw new IllegalStateException(
                            "Invalid MicrosoftAdvertising splits returned by reader");
                }
                MicrosoftAdvertisingSplit recovered = splits.get(0);
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
         * Captures an independent coordinator fallback for the advertising scan.
         *
         * <p>Rows emitted after assignment are checkpointed by the active reader.
         */
        @Override
        public MicrosoftAdvertisingSplit snapshotState(long checkpointId) {
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
