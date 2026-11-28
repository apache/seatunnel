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

package org.apache.seatunnel.connectors.seatunnel.datadog;

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
 * Produces one bounded, recoverable Datadog resource scan with a stable four-column row schema.
 * Endpoint-specific fields remain in JSON rather than changing schemas during pagination.
 */
public class DatadogSource implements SeaTunnelSource<SeaTunnelRow, DatadogSplit, DatadogSplit> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "Datadog";
    private final DatadogConfig config;

    public DatadogSource(ReadonlyConfig options) {
        config = new DatadogConfig(options);
    }

    /**
     * Reject streaming jobs because each independent scan has fixed bounds and no shared watermark.
     */
    @Override
    public void setJobContext(JobContext context) {
        if (JobMode.STREAMING.equals(context.getJobMode())) {
            throw new IllegalArgumentException("Datadog source supports BATCH mode only.");
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

    /**
     * Keep provider JSON inside a string column so endpoint-specific payloads preserve nested
     * values.
     */
    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        TableSchema schema =
                TableSchema.builder()
                        .column(column("resource", false))
                        .column(column("id", true))
                        .column(column("timestamp", true))
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

    /**
     * Each reader owns its client; only the reader assigned the single split opens requests. Idle
     * parallel subtasks complete when the enumerator closes their assignments.
     */
    @Override
    public SourceReader<SeaTunnelRow, DatadogSplit> createReader(SourceReader.Context context) {
        return new DatadogReader(config, context);
    }

    /**
     * Freeze the automatic upper bound on the coordinator and keep it across recovery. The initial
     * fallback also covers checkpoints taken before a reader assignment is persisted.
     */
    @Override
    public SourceSplitEnumerator<DatadogSplit, DatadogSplit> createEnumerator(
            SourceSplitEnumerator.Context<DatadogSplit> context) {
        return new Enumerator(context, config, config.initialSplit(Instant.now()));
    }

    /**
     * Keep the saved coordinator window until the engine returns the reader's committed split. This
     * preserves the original implicit end during coordinated restoration.
     */
    @Override
    public SourceSplitEnumerator<DatadogSplit, DatadogSplit> restoreEnumerator(
            SourceSplitEnumerator.Context<DatadogSplit> context, DatadogSplit state) {
        config.verify(state);
        return new Enumerator(context, config, state);
    }

    /**
     * Keeps a fallback even before a reader has persisted an assignment. During coordinated restore
     * the reader's committed position replaces it, including a terminal reader split.
     */
    static final class Enumerator implements SourceSplitEnumerator<DatadogSplit, DatadogSplit> {
        private final Context<DatadogSplit> context;
        private final DatadogConfig config;
        private final Set<Integer> endedReaders = new HashSet<>();
        // Frozen initial state remains available when an early checkpoint has no reader assignment.
        private DatadogSplit fallback;
        // Protect single-split ownership; returned reader state explicitly clears this flag.
        private boolean hasAssigned;
        // Registration can precede run; all dispatch/state transitions share the context monitor.
        private boolean running;

        Enumerator(Context<DatadogSplit> context, DatadogConfig config, DatadogSplit fallback) {
            this.context = context;
            this.config = config;
            this.fallback = fallback.copy();
        }

        @Override
        public void open() {}

        /**
         * Dispatch after engine registration; late restored state is handled by addSplitsBack. All
         * coordinator ownership transitions use the same context monitor.
         */
        @Override
        public void run() {
            synchronized (context) {
                running = true;
                for (int reader : context.registeredReaders()) {
                    dispatch(reader);
                }
            }
        }

        /**
         * Empty early checkpoints leave the frozen fallback intact. A returned terminal split must
         * also replace that fallback so restoration cannot restart the scan.
         */
        @Override
        public void addSplitsBack(List<DatadogSplit> splits, int subtaskId) {
            synchronized (context) {
                if (splits.isEmpty()) {
                    return;
                }
                if (splits.size() != 1 || subtaskId != 0) {
                    throw new IllegalStateException("Invalid restored Datadog split assignment.");
                }
                config.verify(splits.get(0));
                fallback = splits.get(0).copy();
                hasAssigned = false;
                if (running && context.registeredReaders().contains(0)) {
                    dispatch(0);
                }
            }
        }

        /**
         * Only reader zero owns the scan; other subtasks can finish without opening a request.
         * Registration may happen before run and must not assign the active split prematurely.
         */
        private void dispatch(int reader) {
            if (!running) {
                return;
            }
            if (reader == 0 && !hasAssigned) {
                context.assignSplit(reader, fallback.copy());
                hasAssigned = true;
            } else if (reader != 0 && endedReaders.add(reader)) {
                context.signalNoMoreSplits(reader);
            }
        }

        @Override
        public int currentUnassignedSplitSize() {
            synchronized (context) {
                return hasAssigned ? 0 : 1;
            }
        }

        @Override
        public void registerReader(int subtaskId) {
            synchronized (context) {
                dispatch(subtaskId);
            }
        }

        @Override
        public void handleSplitRequest(int subtaskId) {
            registerReader(subtaskId);
        }

        /**
         * Reader snapshots hold committed progress; this fallback preserves the frozen scan scope.
         */
        @Override
        public DatadogSplit snapshotState(long checkpointId) {
            synchronized (context) {
                return fallback.copy();
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void close() {}
    }
}
