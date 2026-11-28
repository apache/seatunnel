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

package org.apache.seatunnel.connectors.seatunnel.blogger;

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
 * Exposes one Blogger collection to the Source V2 SPI.
 *
 * <p>Each resource uses one ordered scan of its authenticated blog, post, page or comment path.
 */
public final class BloggerSource
        implements SeaTunnelSource<SeaTunnelRow, BloggerSource.Cursor, BloggerSource.State> {
    static final String IDENTIFIER = "Blogger";
    private static final long serialVersionUID = 1L;
    private final BloggerConfig config;
    private final CatalogTable table;

    BloggerSource(BloggerConfig config) {
        this.config = config;
        table =
                CatalogTableUtil.getCatalogTable(
                        config.resource.id,
                        new SeaTunnelRowType(
                                new String[] {"blog_id", "post_id", "data"},
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
    public SourceReader<SeaTunnelRow, Cursor> createReader(SourceReader.Context context) {
        return new BloggerReader(config, context, table.getTablePath().toString());
    }

    /**
     * Defers network work until the reader receives its split.
     *
     * @param context the source coordinator's engine context
     * @return the coordinator with an initial pending split
     */
    @Override
    public SourceSplitEnumerator<Cursor, State> createEnumerator(
            SourceSplitEnumerator.Context<Cursor> context) {
        List<PageCheckpoint> positions =
                Collections.nCopies(config.resource.stages.size(), PageCheckpoint.first());
        Cursor first = new Cursor(config.scope, positions, false);
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
    public SourceSplitEnumerator<Cursor, State> restoreEnumerator(
            SourceSplitEnumerator.Context<Cursor> context, State checkpointState) {
        return new Enumerator(config, context, checkpointState);
    }

    /**
     * Position in one paginated provider collection.
     *
     * <p>Previous tokens are stored as hashes; the active token is required for the next request.
     */
    static final class PageCheckpoint implements Serializable {
        private static final long serialVersionUID = 1L;
        final String token;
        final int pages;
        final int offset;
        final String digest;
        final List<String> seen;

        PageCheckpoint(String token, int pages, int offset, String digest, List<String> seen) {
            this.token = token;
            this.pages = pages;
            this.offset = offset;
            this.digest = digest;
            this.seen = List.copyOf(seen);
        }

        static PageCheckpoint first() {
            return new PageCheckpoint(null, 0, 0, null, List.of());
        }
    }

    /**
     * Checkpoint state of every level of the active traversal.
     *
     * <p>Record bodies and OAuth credentials are excluded. A terminal cursor cannot start a new
     * request.
     */
    public static final class Cursor implements SourceSplit {
        private static final long serialVersionUID = 1L;
        final String scope;
        final List<PageCheckpoint> positions;
        final boolean finished;

        Cursor(String scope, List<PageCheckpoint> positions, boolean finished) {
            this.scope = scope;
            this.positions = List.copyOf(positions);
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
        final Cursor pending;

        State(String scope, Cursor pending) {
            this.scope = scope;
            this.pending = pending;
        }
    }

    /**
     * Assigns or reassigns the sole traversal split.
     *
     * <p>Assignment and checkpoint capture share the same monitor.
     */
    private static final class Enumerator implements SourceSplitEnumerator<Cursor, State> {
        private final BloggerConfig config;
        private final Context<Cursor> context;
        private Cursor pending;
        private boolean started;

        Enumerator(BloggerConfig config, Context<Cursor> context, State checkpoint) {
            if (context.currentParallelism() != 1) {
                throw BloggerConfig.invalid("source parallelism must be 1");
            }
            if (checkpoint == null || !config.scope.equals(checkpoint.scope)) {
                throw BloggerConfig.invalid(
                        "OAuth principal, resource or page_size changed during restore");
            }
            this.config = config;
            this.context = context;
            pending = checkpoint.pending;
            if (pending != null) {
                BloggerReader.validate(config, pending);
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
        public synchronized void addSplitsBack(List<Cursor> splits, int subtaskId) {
            if (splits.isEmpty()) {
                return;
            }
            if (subtaskId != 0 || splits.size() != 1 || pending != null) {
                throw BloggerConfig.invalid("expected one returned Blogger traversal");
            }
            Cursor restored = splits.get(0);
            BloggerReader.validate(config, restored);
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
