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

package org.apache.seatunnel.connectors.seatunnel.appcues;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * Publishes the resource, identifier, normalized cursor, and exact JSON record as one table. One
 * batch reader processes the configured resources without server-side pagination.
 */
public final class AppcuesSource
        implements SeaTunnelSource<SeaTunnelRow, AppcuesSource.Split, AppcuesSource.State> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "Appcues";
    private final AppcuesConfig config;
    private final CatalogTable table;

    AppcuesSource(AppcuesConfig config) {
        this.config = config;
        this.table =
                CatalogTableUtil.getCatalogTable(
                        IDENTIFIER,
                        new SeaTunnelRowType(
                                new String[] {"resource", "id", "cursor_time", "data"},
                                new SeaTunnelDataType<?>[] {
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE
                                }));
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(table);
    }

    @Override
    public SourceReader<SeaTunnelRow, Split> createReader(SourceReader.Context context) {
        return new AppcuesReader(config, context, table.getTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<Split, State> createEnumerator(
            SourceSplitEnumerator.Context<Split> context) {
        Instant end = config.initialEnd();
        Split first = new Split(config.fingerprint, end, 0, 0, null);
        return new Enumerator(config, context, new State(config.fingerprint, end, first));
    }

    @Override
    public SourceSplitEnumerator<Split, State> restoreEnumerator(
            SourceSplitEnumerator.Context<Split> context, State state) {
        return new Enumerator(config, context, state);
    }

    /**
     * Carries the frozen end and the next resource and row position without storing provider data.
     * An optional digest identifies a partially consumed resource.
     */
    static final class Split implements SourceSplit {
        private static final long serialVersionUID = 1L;
        final String fingerprint;
        final Instant end;
        final int resource;
        final int offset;
        final String digest;

        Split(String fingerprint, Instant end, int resource, int offset, String digest) {
            this.fingerprint = fingerprint;
            this.end = end;
            this.resource = resource;
            this.offset = offset;
            this.digest = digest;
        }

        @Override
        public String splitId() {
            return "appcues-resource-selection";
        }
    }

    /**
     * Saves the coordinator's window and its possibly unassigned scan for checkpoint recovery. The
     * window remains available for validating returned reader state.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        final String fingerprint;
        final Instant end;
        final Split pending;

        State(String fingerprint, Instant end, Split pending) {
            this.fingerprint = fingerprint;
            this.end = end;
            this.pending = pending;
        }
    }

    /**
     * Assigns the only scan after reader registration and restores the same window on return. Its
     * monitor protects assignment and checkpoint ownership.
     */
    private static final class Enumerator implements SourceSplitEnumerator<Split, State> {
        private final Context<Split> context;
        private final String fingerprint;
        private final Instant end;
        private Split unassigned;
        private boolean started;

        private Enumerator(AppcuesConfig config, Context<Split> context, State state) {
            if (context.currentParallelism() != 1) {
                throw AppcuesConfig.invalid("source parallelism must be 1");
            }
            if (!config.fingerprint.equals(state.fingerprint)
                    || state.end == null
                    || state.end.isBefore(config.start)
                    || (config.configuredEnd != null && !config.configuredEnd.equals(state.end))) {
                throw AppcuesConfig.invalid("query configuration changed during restore");
            }
            this.context = context;
            this.fingerprint = state.fingerprint;
            this.end = state.end;
            this.unassigned = state.pending;
        }

        @Override
        public void open() {}

        @Override
        public synchronized void run() {
            started = true;
            assign();
        }

        @Override
        public synchronized void close() {
            started = false;
        }

        @Override
        public synchronized void addSplitsBack(List<Split> splits, int reader) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || reader != 0) {
                throw AppcuesConfig.invalid("expected one returned scan");
            }
            Split returned = splits.get(0);
            if (!fingerprint.equals(returned.fingerprint) || !end.equals(returned.end)) {
                throw AppcuesConfig.invalid("query configuration changed during restore");
            }
            unassigned = returned;
            assign();
        }

        @Override
        public synchronized int currentUnassignedSplitSize() {
            return unassigned == null ? 0 : 1;
        }

        @Override
        public synchronized void handleSplitRequest(int subtaskId) {
            assign();
        }

        @Override
        public synchronized void registerReader(int subtaskId) {
            assign();
        }

        @Override
        public synchronized State snapshotState(long checkpointId) {
            return new State(fingerprint, end, unassigned);
        }

        private void assign() {
            if (started && unassigned != null && context.registeredReaders().contains(0)) {
                context.assignSplit(0, unassigned);
                unassigned = null;
                context.signalNoMoreSplits(0);
            }
        }
    }
}
