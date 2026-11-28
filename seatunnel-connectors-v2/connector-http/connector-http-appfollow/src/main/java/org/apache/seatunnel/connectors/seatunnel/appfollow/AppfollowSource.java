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

package org.apache.seatunnel.connectors.seatunnel.appfollow;

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
import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowData.Binding;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Exposes a bounded AppFollow table with explicit app and collection context. One reader traverses
 * a selected resource graph and records its progress for recovery.
 */
public final class AppfollowSource
        implements SeaTunnelSource<SeaTunnelRow, AppfollowSource.Cursor, AppfollowSource.State> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "Appfollow";
    private final AppfollowConfig config;
    private final CatalogTable table;

    AppfollowSource(AppfollowConfig config) {
        this.config = config;
        table =
                CatalogTableUtil.getCatalogTable(
                        IDENTIFIER,
                        new SeaTunnelRowType(
                                new String[] {"resource", "app_collection_id", "ext_id", "data"},
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
    public SourceReader<SeaTunnelRow, Cursor> createReader(SourceReader.Context context) {
        return new AppfollowReader(config, context, table.getTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<Cursor, State> createEnumerator(
            SourceSplitEnumerator.Context<Cursor> context) {
        Cursor pending =
                new Cursor(
                        config.fingerprint,
                        0,
                        0,
                        0,
                        null,
                        Collections.emptyList(),
                        Collections.emptyList());
        return new Enumerator(config, context, new State(config.fingerprint, pending));
    }

    @Override
    public SourceSplitEnumerator<Cursor, State> restoreEnumerator(
            SourceSplitEnumerator.Context<Cursor> context, State state) {
        return new Enumerator(config, context, state);
    }

    /**
     * Stores the next resource, parent index and row offset with any completed discovery IDs. A
     * digest is present only for an unfinished response with already emitted rows.
     */
    static final class Cursor implements SourceSplit {
        private static final long serialVersionUID = 1L;
        final String fingerprint;
        final int stage;
        final int parent;
        final int row;
        final String digest;
        final List<String> collections;
        final List<Binding> bindings;

        Cursor(
                String fingerprint,
                int stage,
                int parent,
                int row,
                String digest,
                List<String> collections,
                List<Binding> bindings) {
            this.fingerprint = fingerprint;
            this.stage = stage;
            this.parent = parent;
            this.row = row;
            this.digest = digest;
            this.collections = Collections.unmodifiableList(new ArrayList<>(collections));
            this.bindings = Collections.unmodifiableList(new ArrayList<>(bindings));
        }

        @Override
        public String splitId() {
            return "appfollow-resource-selection";
        }
    }

    /**
     * Retains the scan if it has not yet been assigned or has been returned on restore. Active
     * reader checkpoints store the progression after assignment.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        final String fingerprint;
        final Cursor pending;

        State(String fingerprint, Cursor pending) {
            this.fingerprint = fingerprint;
            this.pending = pending;
        }
    }

    /**
     * Assigns the single scan to reader zero and rejects restored selections from a different
     * query. Its monitor protects assignment transitions and checkpoint publication.
     */
    private static final class Enumerator implements SourceSplitEnumerator<Cursor, State> {
        private final Context<Cursor> context;
        private final String fingerprint;
        private Cursor pending;
        private boolean started;

        Enumerator(AppfollowConfig config, Context<Cursor> context, State state) {
            if (context.currentParallelism() != 1) {
                throw AppfollowConfig.invalid("source parallelism must be 1");
            }
            this.context = context;
            this.fingerprint = config.fingerprint;
            verify(state.fingerprint);
            this.pending = state.pending;
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
            if (splits.size() != 1 || subtaskId != 0) {
                throw AppfollowConfig.invalid("expected one returned scan");
            }
            verify(splits.get(0).fingerprint);
            pending = splits.get(0);
            assign();
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
        public synchronized int currentUnassignedSplitSize() {
            return pending == null ? 0 : 1;
        }

        @Override
        public synchronized State snapshotState(long checkpointId) {
            return new State(fingerprint, pending);
        }

        private void assign() {
            if (started && pending != null && context.registeredReaders().contains(0)) {
                context.assignSplit(0, pending);
                pending = null;
                context.signalNoMoreSplits(0);
            }
        }

        private void verify(String value) {
            if (!fingerprint.equals(value)) {
                throw AppfollowConfig.invalid("query configuration changed during restore");
            }
        }
    }
}
