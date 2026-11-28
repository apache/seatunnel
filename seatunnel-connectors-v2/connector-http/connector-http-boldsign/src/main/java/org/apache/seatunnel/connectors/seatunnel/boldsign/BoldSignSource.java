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

package org.apache.seatunnel.connectors.seatunnel.boldsign;

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
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignConfig.Window;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignPagination.Cursor;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

/**
 * Exposes one BoldSign collection to the Source V2 SPI.
 *
 * <p>Each resource uses one ordered scan of its authenticated collection.
 */
public final class BoldSignSource
        implements SeaTunnelSource<SeaTunnelRow, BoldSignSource.Split, BoldSignSource.State> {
    static final String IDENTIFIER = "BoldSign";
    private static final long serialVersionUID = 1L;
    private final BoldSignConfig config;
    private final CatalogTable table;

    BoldSignSource(BoldSignConfig config) {
        this.config = config;
        table =
                CatalogTableUtil.getCatalogTable(
                        config.resource.id,
                        new SeaTunnelRowType(
                                new String[] {"brand_id", "data"},
                                new SeaTunnelDataType<?>[] {
                                    BasicType.STRING_TYPE, BasicType.STRING_TYPE
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
        return new BoldSignReader(config, context, table.getTablePath().toString());
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
                        Cursor.first(config.resource),
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
     * Stores an optional brand offset and the source position within the active list page.
     *
     * <p>Frozen dates and identity digests allow safe replay without storing provider payloads or
     * credentials.
     */
    public static final class Split implements SourceSplit {
        private static final long serialVersionUID = 1L;
        final String scope;
        final Window window;
        final int brandOffset;
        final String brandDigest;
        final Cursor position;
        final int offset;
        final String digest;
        final boolean finished;

        Split(
                String scope,
                Window window,
                int brandOffset,
                String brandDigest,
                Cursor position,
                int offset,
                String digest,
                boolean finished) {
            this.scope = scope;
            this.window = window;
            this.brandOffset = brandOffset;
            this.brandDigest = brandDigest;
            this.position = position;
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
        private final BoldSignConfig config;
        private final Context<Split> context;
        private Split pending;
        private boolean started;

        Enumerator(BoldSignConfig config, Context<Split> context, State checkpoint) {
            if (context.currentParallelism() != 1) {
                throw BoldSignConfig.invalid("source parallelism must be 1");
            }
            if (checkpoint == null || !config.scope.equals(checkpoint.scope)) {
                throw BoldSignConfig.invalid(
                        "account, region, resource, date window or page_size changed during restore");
            }
            this.config = config;
            this.context = context;
            pending = checkpoint.pending;
            if (pending != null) {
                BoldSignReader.validate(config, pending);
            }
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
                throw BoldSignConfig.invalid("expected one returned BoldSign traversal");
            }
            Split restored = splits.get(0);
            BoldSignReader.validate(config, restored);
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
