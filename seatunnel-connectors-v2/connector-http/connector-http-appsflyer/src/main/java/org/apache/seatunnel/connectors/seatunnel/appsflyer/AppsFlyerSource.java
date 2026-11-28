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

package org.apache.seatunnel.connectors.seatunnel.appsflyer;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Exposes all report views in a single bounded table. The plan is frozen once and assigned to one
 * reader so splitting and quota decisions stay ordered.
 */
public final class AppsFlyerSource
        implements SeaTunnelSource<SeaTunnelRow, AppsFlyerSource.Cursor, AppsFlyerSource.State> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "AppsFlyer";
    private final AppsFlyerConfig config;
    private final CatalogTable table;

    AppsFlyerSource(AppsFlyerConfig config) {
        this.config = config;
        table =
                CatalogTableUtil.getCatalogTable(
                        IDENTIFIER,
                        new SeaTunnelRowType(
                                new String[] {
                                    "resource", "app_id", "report_time", "time_zone", "data"
                                },
                                new SeaTunnelDataType<?>[] {
                                    BasicType.STRING_TYPE,
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
        return new AppsFlyerReader(config, context, table.getTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<Cursor, State> createEnumerator(
            SourceSplitEnumerator.Context<Cursor> context) {
        Cursor pending =
                new Cursor(
                        config.fingerprint, AppsFlyerWindow.plan(config, Instant.now()), 0, null);
        return new Enumerator(config, context, new State(config.fingerprint, pending));
    }

    @Override
    public SourceSplitEnumerator<Cursor, State> restoreEnumerator(
            SourceSplitEnumerator.Context<Cursor> context, State state) {
        return new Enumerator(config, context, state);
    }

    /**
     * Saves the remaining actual time ranges and the next row of the first unfinished range. The
     * token, signed redirect URLs and report contents are deliberately absent.
     */
    static final class Cursor implements SourceSplit {
        private static final long serialVersionUID = 1L;
        final String fingerprint;
        final List<AppsFlyerWindow> windows;
        final int row;
        final String digest;

        Cursor(String fingerprint, List<AppsFlyerWindow> windows, int row, String digest) {
            this.fingerprint = fingerprint;
            this.windows = Collections.unmodifiableList(new ArrayList<>(windows));
            this.row = row;
            this.digest = digest;
        }

        @Override
        public String splitId() {
            return "apps-flyer-report-job";
        }
    }

    /**
     * Retains the initial scan if the coordinator has not yet assigned it to the reader. Once
     * assigned, the reader checkpoint owns progress and any adaptive subdivisions.
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
     * Assigns the pending scan to reader zero, including any returned checkpoint. The monitor
     * serializes assignment and capture of coordinator state.
     */
    private static final class Enumerator implements SourceSplitEnumerator<Cursor, State> {
        private final Context<Cursor> context;
        private final String fingerprint;
        private Cursor pending;
        private boolean started;

        Enumerator(AppsFlyerConfig config, Context<Cursor> context, State state) {
            if (context.currentParallelism() != 1) {
                throw AppsFlyerConfig.invalid("source parallelism must be 1");
            }
            this.context = context;
            fingerprint = config.fingerprint;
            verify(state.fingerprint);
            pending = state.pending;
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
                throw AppsFlyerConfig.invalid("expected one returned scan");
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
                throw AppsFlyerConfig.invalid("query configuration changed during restore");
            }
        }
    }
}
