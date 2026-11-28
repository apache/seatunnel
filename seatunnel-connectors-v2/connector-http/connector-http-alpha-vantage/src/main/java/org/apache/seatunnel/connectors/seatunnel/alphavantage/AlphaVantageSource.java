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

package org.apache.seatunnel.connectors.seatunnel.alphavantage;

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
 * Publishes the fixed trading schema and assigns the selected provider functions to one batch
 * reader so they share a single request budget.
 */
public final class AlphaVantageSource
        implements SeaTunnelSource<
                SeaTunnelRow, AlphaVantageSource.Split, AlphaVantageSource.State> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "AlphaVantage";
    private final AlphaVantageConfig config;
    private final CatalogTable table;

    AlphaVantageSource(AlphaVantageConfig config) {
        this.config = config;
        this.table =
                CatalogTableUtil.getCatalogTable(
                        IDENTIFIER,
                        new SeaTunnelRowType(
                                new String[] {
                                    "resource", "symbol", "date", "timestamp", "time_zone", "data"
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
        return new AlphaVantageReader(config, context, table.getTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<Split, State> createEnumerator(
            SourceSplitEnumerator.Context<Split> context) {
        return new Enumerator(
                config,
                context,
                new State(config.fingerprint, new Split(config.fingerprint, 0, 0, null)));
    }

    @Override
    public SourceSplitEnumerator<Split, State> restoreEnumerator(
            SourceSplitEnumerator.Context<Split> context, State state) {
        return new Enumerator(config, context, state);
    }

    /**
     * Persists the next resource or row position. A partly emitted response also records its
     * identity so restore can check for changes before applying the row offset.
     */
    static final class Split implements SourceSplit {
        private static final long serialVersionUID = 1L;
        final String fingerprint;
        final int resource;
        final int offset;
        final String responseId;

        Split(String fingerprint, int resource, int offset, String responseId) {
            this.fingerprint = fingerprint;
            this.resource = resource;
            this.offset = offset;
            this.responseId = responseId;
        }

        @Override
        public String splitId() {
            return "alpha-vantage-selection";
        }
    }

    /**
     * Stores whether the coordinator assigned the selection before checkpoint. Once assigned, the
     * reader checkpoint owns the query position.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String fingerprint;
        private final Split pending;

        State(String fingerprint, Split pending) {
            this.fingerprint = fingerprint;
            this.pending = pending;
        }
    }

    /**
     * Transfers the selection to reader zero. During restore, the coordinator can receive the
     * reader's split back before assigning the selection again.
     */
    private static final class Enumerator implements SourceSplitEnumerator<Split, State> {
        private final AlphaVantageConfig config;
        private final Context<Split> context;
        private Split pending;
        private boolean started;

        private Enumerator(AlphaVantageConfig config, Context<Split> context, State state) {
            if (context.currentParallelism() != 1) {
                throw AlphaVantageConfig.invalid("source parallelism must be 1");
            }
            this.config = config;
            this.context = context;
            check(state.fingerprint);
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
        public synchronized void addSplitsBack(List<Split> splits, int reader) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || reader != 0) {
                throw AlphaVantageConfig.invalid("expected one returned selection");
            }
            check(splits.get(0).fingerprint);
            pending = splits.get(0);
            assign();
        }

        @Override
        public synchronized int currentUnassignedSplitSize() {
            return pending == null ? 0 : 1;
        }

        @Override
        public synchronized void handleSplitRequest(int reader) {
            assign();
        }

        @Override
        public synchronized void registerReader(int reader) {
            assign();
        }

        @Override
        public synchronized State snapshotState(long checkpoint) {
            return new State(config.fingerprint, pending);
        }

        private void assign() {
            if (started && pending != null && context.registeredReaders().contains(0)) {
                context.assignSplit(0, pending);
                pending = null;
                context.signalNoMoreSplits(0);
            }
        }

        private void check(String fingerprint) {
            if (!config.fingerprint.equals(fingerprint)) {
                throw AlphaVantageConfig.invalid("query configuration changed during restore");
            }
        }
    }
}
