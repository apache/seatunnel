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

package org.apache.seatunnel.connectors.seatunnel.awscloudtrail;

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
import org.apache.seatunnel.connectors.seatunnel.awscloudtrail.AwsCloudTrailConfig.Window;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Exposes one bounded management-event lookup to the SeaTunnel source SPI. One ordered reader
 * preserves the regional continuation sequence across checkpoints.
 */
public final class AwsCloudTrailSource
        implements SeaTunnelSource<
                SeaTunnelRow, AwsCloudTrailSource.Cursor, AwsCloudTrailSource.State> {
    private static final long serialVersionUID = 1L;
    private final AwsCloudTrailConfig config;
    private final CatalogTable table;

    AwsCloudTrailSource(AwsCloudTrailConfig config) {
        this.config = config;
        this.table =
                CatalogTableUtil.getCatalogTable(
                        AwsCloudTrailConfig.RESOURCE,
                        new SeaTunnelRowType(
                                new String[] {
                                    "resource",
                                    "region",
                                    "event_id",
                                    "event_time",
                                    "cloud_trail_event",
                                    "data"
                                },
                                new SeaTunnelDataType<?>[] {
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE
                                }));
    }

    @Override
    public String getPluginName() {
        return AwsCloudTrailConfig.IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(table);
    }

    @Override
    public SourceReader<SeaTunnelRow, Cursor> createReader(SourceReader.Context context) {
        return new AwsCloudTrailReader(config, context, table.getTablePath().toString());
    }

    /**
     * Freezes the lookup dates once before assigning the ordered scan. Coordinator state retains
     * this interval if a checkpoint occurs before reader assignment.
     */
    @Override
    public SourceSplitEnumerator<Cursor, State> createEnumerator(
            SourceSplitEnumerator.Context<Cursor> context) {
        Window window = config.freeze(Instant.now());
        return new Enumerator(
                config,
                context,
                new State(
                        config.identity,
                        new Cursor(
                                config.identity,
                                window,
                                null,
                                List.of(),
                                0,
                                0,
                                null,
                                null,
                                false)));
    }

    @Override
    public SourceSplitEnumerator<Cursor, State> restoreEnumerator(
            SourceSplitEnumerator.Context<Cursor> context, State state) {
        return new Enumerator(config, context, state);
    }

    /**
     * Records the exact window, current continuation and partial row offset when it exists. An
     * optional page digest prevents changed content or order from shifting the offset.
     */
    public static final class Cursor implements SourceSplit {
        private static final long serialVersionUID = 1L;
        final String identity;
        final Window window;
        final String token;
        final List<String> tokenHashes;
        final int completedPages;
        final int offset;
        final String digest;
        final BigDecimal preceding;
        final boolean finished;

        Cursor(
                String identity,
                Window window,
                String token,
                List<String> tokenHashes,
                int completedPages,
                int offset,
                String digest,
                BigDecimal preceding,
                boolean finished) {
            this.identity = identity;
            this.window = window;
            this.token = token;
            this.tokenHashes = Collections.unmodifiableList(new ArrayList<>(tokenHashes));
            this.completedPages = completedPages;
            this.offset = offset;
            this.digest = digest;
            this.preceding = preceding;
            this.finished = finished;
        }

        @Override
        public String splitId() {
            return "cloudtrail-management-events";
        }
    }

    /**
     * Holds a pending scan before assignment and the logical credential/query identity afterward.
     * It contains no credential secrets.
     */
    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        final String identity;
        final Cursor pending;

        State(String identity, Cursor pending) {
            this.identity = identity;
            this.pending = pending;
        }
    }

    /**
     * Assigns an initial or returned scan exactly once to the reader for this source. Assignment
     * and checkpoint capture share the coordinator monitor.
     */
    private static final class Enumerator implements SourceSplitEnumerator<Cursor, State> {
        private final String identity;
        private final Context<Cursor> context;
        private Cursor pending;
        private boolean started;

        Enumerator(AwsCloudTrailConfig config, Context<Cursor> context, State state) {
            if (context.currentParallelism() != 1) {
                throw AwsCloudTrailConfig.invalid("source parallelism must be 1");
            }
            identity = config.identity;
            verify(state.identity);
            if (state.pending != null) {
                verify(state.pending.identity);
            }
            pending = state.pending;
            this.context = context;
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
        public synchronized void addSplitsBack(List<Cursor> splits, int subtaskId) {
            if (splits.isEmpty()) {
                return;
            }
            if (subtaskId != 0 || splits.size() != 1) {
                throw AwsCloudTrailConfig.invalid("expected one returned lookup scan");
            }
            verify(splits.get(0).identity);
            pending = splits.get(0);
            assign();
        }

        @Override
        public synchronized int currentUnassignedSplitSize() {
            return pending == null ? 0 : 1;
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
            return new State(identity, pending);
        }

        private void verify(String fingerprint) {
            if (!identity.equals(fingerprint)) {
                throw AwsCloudTrailConfig.invalid(
                        "lookup account, region or parameters changed during restore");
            }
        }

        private void assign() {
            if (started && pending != null && context.registeredReaders().contains(0)) {
                context.assignSplit(0, pending);
                pending = null;
                context.signalNoMoreSplits(0);
            }
        }
    }
}
