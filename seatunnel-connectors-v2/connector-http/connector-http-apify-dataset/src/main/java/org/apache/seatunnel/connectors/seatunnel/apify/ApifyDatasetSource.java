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

package org.apache.seatunnel.connectors.seatunnel.apify;

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
import java.util.Collections;
import java.util.List;

/**
 * Publishes the dataset table and assigns its ordered, bounded scan to one reader. Serial offsets
 * are required because each resource establishes its own first-page boundary.
 */
public final class ApifyDatasetSource
        implements SeaTunnelSource<
                SeaTunnelRow, ApifyDatasetSource.Cursor, ApifyDatasetSource.State> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "ApifyDataset";
    private final ApifyDatasetConfig config;
    private final CatalogTable table;

    ApifyDatasetSource(ApifyDatasetConfig config) {
        this.config = config;
        this.table =
                CatalogTableUtil.getCatalogTable(
                        IDENTIFIER,
                        new SeaTunnelRowType(
                                new String[] {"resource", "dataset_id", "payload"},
                                new SeaTunnelDataType<?>[] {
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE
                                }));
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
        return Collections.singletonList(table);
    }

    @Override
    public SourceReader<SeaTunnelRow, Cursor> createReader(SourceReader.Context context) {
        return new ApifyDatasetReader(config, context, table.getTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<Cursor, State> createEnumerator(
            SourceSplitEnumerator.Context<Cursor> context) {
        return new Enumerator(
                config,
                context,
                new State(config.fingerprint, Cursor.start(config.fingerprint, 0)));
    }

    @Override
    public SourceSplitEnumerator<Cursor, State> restoreEnumerator(
            SourceSplitEnumerator.Context<Cursor> context, State state) {
        return new Enumerator(config, context, state);
    }

    /**
     * Stores the next resource, scan offset and fixed batch boundary with an optional partial page.
     * The stored counters and fingerprints omit literal credentials, names, and response bodies.
     */
    static final class Cursor implements SourceSplit {
        private static final long serialVersionUID = 1L;
        final String fingerprint;
        final int resourceIndex;
        final int completedPages;
        final long offset;
        final long end;
        final int rowOffset;
        final String pageDigest;

        Cursor(
                String fingerprint,
                int resourceIndex,
                int completedPages,
                long offset,
                long end,
                int rowOffset,
                String pageDigest) {
            this.fingerprint = fingerprint;
            this.resourceIndex = resourceIndex;
            this.completedPages = completedPages;
            this.offset = offset;
            this.end = end;
            this.rowOffset = rowOffset;
            this.pageDigest = pageDigest;
        }

        static Cursor start(String fingerprint, int resourceIndex) {
            return new Cursor(fingerprint, resourceIndex, 0, 0, -1, 0, null);
        }

        @Override
        public String splitId() {
            return "apify-dataset-selection";
        }
    }

    /**
     * Records the coordinator's ownership of an unassigned or returned scan checkpoint. After
     * successful assignment, the reader is responsible for saving the position.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        final String fingerprint;
        final Cursor unassigned;

        State(String fingerprint, Cursor unassigned) {
            this.fingerprint = fingerprint;
            this.unassigned = unassigned;
        }
    }

    /**
     * Assigns one bounded scan to reader zero after registration and handles split returns on
     * restore. All assignment and checkpoint operations use the enumerator monitor.
     */
    private static final class Enumerator implements SourceSplitEnumerator<Cursor, State> {
        private final String fingerprint;
        private final Context<Cursor> context;
        private Cursor pending;
        private boolean started;

        private Enumerator(ApifyDatasetConfig config, Context<Cursor> context, State state) {
            if (context.currentParallelism() != 1) {
                throw ApifyDatasetConfig.invalid("source parallelism must be 1");
            }
            this.context = context;
            this.fingerprint = config.fingerprint;
            verifyFingerprint(state.fingerprint);
            pending = state.unassigned;
        }

        @Override
        public void open() {}

        @Override
        public synchronized void run() {
            started = true;
            assignIfReady();
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
            if (splits.size() != 1 || subtaskId != 0) {
                throw ApifyDatasetConfig.invalid("expected one returned scan");
            }
            Cursor returned = splits.get(0);
            verifyFingerprint(returned.fingerprint);
            pending = returned;
            assignIfReady();
        }

        @Override
        public synchronized void handleSplitRequest(int subtaskId) {
            assignIfReady();
        }

        @Override
        public synchronized void registerReader(int subtaskId) {
            assignIfReady();
        }

        @Override
        public synchronized int currentUnassignedSplitSize() {
            return pending == null ? 0 : 1;
        }

        @Override
        public synchronized State snapshotState(long checkpointId) {
            return new State(fingerprint, pending);
        }

        private void assignIfReady() {
            if (started && pending != null && context.registeredReaders().contains(0)) {
                context.assignSplit(0, pending);
                pending = null;
                context.signalNoMoreSplits(0);
            }
        }

        private void verifyFingerprint(String value) {
            if (!fingerprint.equals(value)) {
                throw ApifyDatasetConfig.invalid("query configuration changed during restore");
            }
        }
    }
}
