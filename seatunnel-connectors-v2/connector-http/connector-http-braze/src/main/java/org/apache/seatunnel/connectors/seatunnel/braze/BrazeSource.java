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

package org.apache.seatunnel.connectors.seatunnel.braze;

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
import java.time.LocalDate;
import java.util.List;

/**
 * Exposes one Braze collection to the Source V2 SPI.
 *
 * <p>Each job has one ordered traversal of the resource and any required parent list.
 */
public final class BrazeSource
        implements SeaTunnelSource<SeaTunnelRow, BrazeSource.Split, BrazeSource.State> {
    static final String IDENTIFIER = "Braze";
    private static final long serialVersionUID = 1L;
    private final BrazeConfig config;
    private final CatalogTable table;

    BrazeSource(BrazeConfig config) {
        this.config = config;
        table =
                CatalogTableUtil.getCatalogTable(
                        config.resource.id,
                        new SeaTunnelRowType(
                                new String[] {"entity_id", "time", "data"},
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
     * Creates a worker for the sole traversal split.
     *
     * @param context the source worker's engine context
     * @return the source reader
     */
    @Override
    public SourceReader<SeaTunnelRow, Split> createReader(SourceReader.Context context) {
        return new BrazeReader(config, context, table.getTablePath().toString());
    }

    /**
     * Defers network work until the reader receives its split.
     *
     * @param context the source coordinator's engine context
     * @return the coordinator with an initial pending split
     */
    @Override
    public SourceSplitEnumerator<Split, State> createEnumerator(
            SourceSplitEnumerator.Context<Split> context) {
        Split first =
                new Split(
                        config.scope,
                        config.freeze(Instant.now()),
                        0,
                        null,
                        null,
                        config.resource.days == 0 ? null : config.start,
                        0,
                        0,
                        null,
                        false);
        return new Enumerator(config, context, new State(config.scope, first));
    }

    /**
     * Reuses the pending traversal if the coordinator was checkpointed before assignment.
     *
     * @param context the source coordinator's engine context
     * @param checkpointState saved assignment status
     * @return the restored coordinator
     */
    @Override
    public SourceSplitEnumerator<Split, State> restoreEnumerator(
            SourceSplitEnumerator.Context<Split> context, State checkpointState) {
        return new Enumerator(config, context, checkpointState);
    }

    /**
     * Stores the next page, parent and record position in a frozen traversal.
     *
     * <p>Digests permit safe replay without storing provider payloads, names or credentials.
     */
    public static final class Split implements SourceSplit {
        private static final long serialVersionUID = 1L;
        final String scope;
        final Instant cutoff;
        final int page;
        final String previousList;
        final String parentList;
        final LocalDate from;
        final int parentOffset;
        final int offset;
        final String records;
        final boolean finished;

        Split(
                String scope,
                Instant cutoff,
                int page,
                String previousList,
                String parentList,
                LocalDate from,
                int parentOffset,
                int offset,
                String records,
                boolean finished) {
            this.scope = scope;
            this.cutoff = cutoff;
            this.page = page;
            this.previousList = previousList;
            this.parentList = parentList;
            this.from = from;
            this.parentOffset = parentOffset;
            this.offset = offset;
            this.records = records;
            this.finished = finished;
        }

        @Override
        public String splitId() {
            return IDENTIFIER + "-" + scope;
        }
    }

    /**
     * Stores coordinator assignment status.
     *
     * <p>If the split is already assigned, progress belongs to the reader checkpoint.
     */
    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        final String scope;
        final Split pending;

        State(String scope, Split pending) {
            this.scope = scope;
            this.pending = pending;
        }
    }

    /**
     * Assigns or reassigns the sole traversal split.
     *
     * <p>Assignment and checkpoint capture share the same monitor.
     */
    private static final class Enumerator implements SourceSplitEnumerator<Split, State> {
        private final BrazeConfig config;
        private final Context<Split> context;
        private Split pending;
        private boolean started;

        Enumerator(BrazeConfig config, Context<Split> context, State checkpoint) {
            if (context.currentParallelism() != 1) {
                throw BrazeConfig.invalid("source parallelism must be 1");
            }
            if (checkpoint == null || !config.scope.equals(checkpoint.scope)) {
                throw BrazeConfig.invalid(
                        "workspace, resource or date options changed during restore");
            }
            this.config = config;
            this.context = context;
            pending = checkpoint.pending;
            if (pending != null) {
                BrazeReader.validate(config, pending);
            }
        }

        @Override
        public void open() {}

        /**
         * Makes the pending traversal eligible for assignment.
         *
         * <p>Registration may happen before the coordinator starts.
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
         * Retains a returned split if the reader is not yet ready.
         *
         * @param splits the returned traversal, if any
         * @param subtaskId reader index registered by the engine
         */
        @Override
        public synchronized void addSplitsBack(List<Split> splits, int subtaskId) {
            if (splits.isEmpty()) {
                return;
            }
            if (subtaskId != 0 || splits.size() != 1 || pending != null) {
                throw BrazeConfig.invalid("expected one returned Braze traversal");
            }
            Split restored = splits.get(0);
            BrazeReader.validate(config, restored);
            pending = restored;
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
         * Captures an unassigned traversal while assignment is serialized on the same monitor.
         *
         * @param checkpointId the coordinator checkpoint identifier
         * @return the pending traversal, if the reader does not already own it
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
