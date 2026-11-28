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

package org.apache.seatunnel.connectors.seatunnel.bamboohr;

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
import org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRConfig.Bounds;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/**
 * A bounded BambooHR scan. One reader owns each configured resource's traversal and time window.
 */
public final class BambooHRSource
        implements SeaTunnelSource<SeaTunnelRow, BambooHRSource.Cursor, BambooHRSource.State> {
    static final String IDENTIFIER = "BambooHR";
    private static final long serialVersionUID = 1L;
    private final BambooHRConfig config;
    private final CatalogTable table;

    BambooHRSource(BambooHRConfig config) {
        this.config = config;
        this.table =
                CatalogTableUtil.getCatalogTable(
                        config.resource.id,
                        new SeaTunnelRowType(
                                new String[] {"data", "change_type", "last_changed"},
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
        return List.of(table);
    }

    /**
     * Opens an independent provider traversal for the sole assigned split.
     *
     * <p>The reader uses the selected resource's catalog path for every output row.
     */
    @Override
    public SourceReader<SeaTunnelRow, Cursor> createReader(SourceReader.Context context) {
        return new BambooHRReader(config, context, table.getTablePath().toString());
    }

    /**
     * Freezes the provider window before assignment, including the optional changed-employee fence.
     */
    @Override
    public SourceSplitEnumerator<Cursor, State> createEnumerator(
            SourceSplitEnumerator.Context<Cursor> context) {
        Bounds bounds = config.freeze(Instant.now());
        return new Enumerator(
                config,
                context,
                new State(
                        config.scope,
                        new Cursor(config.scope, bounds, bounds.from, 0, null, false)));
    }

    /**
     * Reuses the saved bounds and initial assignment instead of recomputing the lookback.
     *
     * <p>The saved state fixes dates even when the job is restarted on a later company day.
     */
    @Override
    public SourceSplitEnumerator<Cursor, State> restoreEnumerator(
            SourceSplitEnumerator.Context<Cursor> context, State checkpointState) {
        return new Enumerator(config, context, checkpointState);
    }

    /**
     * Contains only scan position and a digest of the active window or employee discovery list.
     *
     * <p>The digest checks the replay boundary without serializing any employee records.
     */
    public static final class Cursor implements SourceSplit {
        private static final long serialVersionUID = 1L;
        final String scope;
        final Bounds bounds;
        final LocalDate windowStart;
        final int offset;
        final String digest;
        final boolean finished;

        Cursor(
                String scope,
                Bounds bounds,
                LocalDate windowStart,
                int offset,
                String digest,
                boolean finished) {
            this.scope = scope;
            this.bounds = bounds;
            this.windowStart = windowStart;
            this.offset = offset;
            this.digest = digest;
            this.finished = finished;
        }

        @Override
        public String splitId() {
            return IDENTIFIER + "-" + scope;
        }
    }

    /**
     * Retains a pending split if a checkpoint precedes initial assignment.
     *
     * <p>An empty reader checkpoint is valid before that pending split has been delivered.
     */
    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        final String scope;
        final Cursor pending;

        State(String scope, Cursor pending) {
            this.scope = scope;
            this.pending = pending;
        }
    }

    /**
     * Coordinates the one split and the exact handoff of checkpoint position to its reader.
     *
     * <p>Coordinator assignment and snapshot capture share the same monitor.
     */
    private static final class Enumerator implements SourceSplitEnumerator<Cursor, State> {
        private final BambooHRConfig config;
        private final Context<Cursor> context;
        private Cursor pending;
        private boolean started;

        Enumerator(BambooHRConfig config, Context<Cursor> context, State state) {
            if (context.currentParallelism() != 1) {
                throw BambooHRConfig.invalid("source parallelism must be 1");
            }
            if (state == null || !config.scope.equals(state.scope)) {
                throw BambooHRConfig.invalid(
                        "resource, company, fields or dates changed during restore");
            }
            this.config = config;
            this.context = context;
            if (state.pending != null) {
                BambooHRReader.validate(config, state.pending);
            }
            pending = state.pending;
        }

        @Override
        public void open() {}

        /**
         * Starts assignment once the engine has registered its reader.
         *
         * <p>Reader registration may precede or follow the start callback.
         */
        @Override
        public synchronized void run() {
            started = true;
            assign();
        }

        @Override
        public synchronized void close() {
            started = false;
        }

        /**
         * Retains a returned split until its registered reader can consume it again.
         *
         * <p>An already verified returned split retains its original date and source position.
         */
        @Override
        public synchronized void addSplitsBack(List<Cursor> splits, int subtaskId) {
            if (splits.isEmpty()) {
                return;
            }
            if (subtaskId != 0 || splits.size() != 1 || pending != null) {
                throw BambooHRConfig.invalid("expected one returned BambooHR split");
            }
            Cursor returned = splits.get(0);
            BambooHRReader.validate(config, returned);
            pending = returned;
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

        /**
         * Coordinator progress can be captured before or after the reader receives the split.
         *
         * <p>A null pending split indicates that progress has already moved to the reader.
         */
        @Override
        public synchronized State snapshotState(long checkpointId) {
            return new State(config.scope, pending);
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
