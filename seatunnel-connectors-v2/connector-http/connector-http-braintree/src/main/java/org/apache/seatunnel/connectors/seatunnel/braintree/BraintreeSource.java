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
package org.apache.seatunnel.connectors.seatunnel.braintree;

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
 * Bounded source for Braintree gateway entities.
 *
 * <p>The row envelope carries an opaque entity ID, an optional search cursor, and the typed record.
 */
public final class BraintreeSource
        implements SeaTunnelSource<SeaTunnelRow, BraintreeSplit, BraintreeSplit> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "Braintree";

    private final BraintreeConfig config;

    public BraintreeSource(ReadonlyConfig options) {
        config = new BraintreeConfig(options);
    }

    @Override
    public void setJobContext(JobContext context) {
        if (JobMode.STREAMING.equals(context.getJobMode())) {
            throw new IllegalArgumentException("Braintree requires BATCH mode");
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
                        .column(column("resource", false))
                        .column(column("id", false))
                        .column(column("cursor", true))
                        .column(column("record", false))
                        .build();
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, TablePath.DEFAULT),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null));
    }

    private static PhysicalColumn column(String name, boolean nullable) {
        return PhysicalColumn.of(name, BasicType.STRING_TYPE, 0, nullable, null, null);
    }

    @Override
    public SourceReader<SeaTunnelRow, BraintreeSplit> createReader(SourceReader.Context context) {
        return new BraintreeReader(config, context);
    }

    /**
     * Freezes the selected merchant time window before the first provider request.
     *
     * <p>The coordinator retains the pending split for checkpoints before assignment.
     */
    @Override
    public SourceSplitEnumerator<BraintreeSplit, BraintreeSplit> createEnumerator(
            SourceSplitEnumerator.Context<BraintreeSplit> context) {
        return new Enumerator(context, config, config.newSplit(Instant.now()));
    }

    /**
     * Restores the pending merchant scan under the same time, environment and merchant scope.
     *
     * <p>The engine can independently return a newer reader position before assignment.
     */
    @Override
    public SourceSplitEnumerator<BraintreeSplit, BraintreeSplit> restoreEnumerator(
            SourceSplitEnumerator.Context<BraintreeSplit> context, BraintreeSplit checkpoint) {
        checkpoint.validate(config);
        return new Enumerator(context, config, checkpoint);
    }

    /**
     * Keeps the initial scan available before reader assignment and returns scope on reassignment.
     *
     * <p>This covers a coordinator checkpoint taken before the active subtask receives its split.
     */
    static final class Enumerator implements SourceSplitEnumerator<BraintreeSplit, BraintreeSplit> {
        private final Context<BraintreeSplit> context;
        private final BraintreeConfig config;
        private final Set<Integer> inactive = new HashSet<>();
        private BraintreeSplit latest;
        private boolean running;
        private boolean assigned;

        Enumerator(
                Context<BraintreeSplit> context, BraintreeConfig config, BraintreeSplit starting) {
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
        public void addSplitsBack(List<BraintreeSplit> splits, int subtask) {
            synchronized (context) {
                if (splits.isEmpty()) {
                    return;
                }
                if (subtask != 0 || splits.size() != 1) {
                    throw new IllegalStateException("Invalid Braintree splits returned by reader");
                }
                BraintreeSplit recovered = splits.get(0);
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
         * Captures an independent coordinator fallback for the merchant scan.
         *
         * <p>Rows emitted after assignment are checkpointed by the active reader.
         */
        @Override
        public BraintreeSplit snapshotState(long checkpointId) {
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
