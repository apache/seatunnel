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

package org.apache.seatunnel.connectors.seatunnel.websocket.server;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Receives pushed messages at one streaming listener. Only assignment is checkpointed; peers own
 * durable replay/deduplication and a restored reader may bind on another worker. Network workers
 * never emit rows, and typed decoding is admitted separately from the bounded wire queue.
 */
public final class WebSocketServerSource
        implements SeaTunnelSource<
                SeaTunnelRow, WebSocketServerSource.Assignment, WebSocketServerSource.Assignment> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "WebSocketServer";

    private final WebSocketServerSourceOptions.Settings settings;
    private final CatalogTable table;
    private final boolean customSchema;

    public WebSocketServerSource(ReadonlyConfig options) {
        settings = new WebSocketServerSourceOptions.Settings(options);
        customSchema = options.getOptional(ConnectorCommonOptions.SCHEMA).isPresent();
        if ((customSchema && "raw".equals(settings.format))
                || (!customSchema && "json".equals(settings.format))) {
            throw new IllegalArgumentException(
                    "WebSocketServer raw has a fixed schema and json requires a schema");
        }
        if (customSchema) {
            table = CatalogTableUtil.buildWithConfig(options);
        } else {
            TableSchema.Builder schema = TableSchema.builder();
            if ("raw".equals(settings.format)) {
                schema.column(
                                PhysicalColumn.of(
                                        "message_type",
                                        BasicType.STRING_TYPE,
                                        0,
                                        false,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "payload",
                                        PrimitiveByteArrayType.INSTANCE,
                                        0,
                                        false,
                                        null,
                                        null));
            } else {
                schema.column(
                        PhysicalColumn.of("value", BasicType.STRING_TYPE, 0, false, null, null));
            }
            table =
                    CatalogTable.of(
                            TableIdentifier.of(IDENTIFIER, TablePath.DEFAULT),
                            schema.build(),
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            null);
        }
    }

    @Override
    public void setJobContext(JobContext context) {
        if (JobMode.BATCH.equals(context.getJobMode())) {
            throw new IllegalArgumentException("WebSocketServer requires STREAMING mode");
        }
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.UNBOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(table);
    }

    @Override
    public SourceReader<SeaTunnelRow, Assignment> createReader(SourceReader.Context context) {
        return new ListenerReader(context, settings, table, customSchema);
    }

    @Override
    public SourceSplitEnumerator<Assignment, Assignment> createEnumerator(
            SourceSplitEnumerator.Context<Assignment> context) {
        return new Enumerator(context);
    }

    @Override
    public SourceSplitEnumerator<Assignment, Assignment> restoreEnumerator(
            SourceSplitEnumerator.Context<Assignment> context, Assignment checkpoint) {
        return new Enumerator(context);
    }

    /**
     * Serializable listener ownership marker with stable split identity. It never captures sockets,
     * queued payloads, partial messages or replay acknowledgements.
     */
    static final class Assignment implements SourceSplit {
        private static final long serialVersionUID = 1L;

        @Override
        public String splitId() {
            return "websocket-listener";
        }
    }

    /**
     * Assign the endpoint only to reader zero; extra readers never bind. Restore recreates the live
     * assignment rather than recovering old sockets or receive buffers.
     */
    private static final class Enumerator implements SourceSplitEnumerator<Assignment, Assignment> {
        private final Context<Assignment> context;
        private final Set<Integer> idle = new HashSet<>();
        private boolean running;
        private boolean assigned;

        private Enumerator(Context<Assignment> context) {
            this.context = context;
        }

        @Override
        public void open() {}

        @Override
        public void run() {
            synchronized (context) {
                running = true;
                context.registeredReaders().forEach(this::dispatch);
            }
        }

        @Override
        public void registerReader(int reader) {
            synchronized (context) {
                dispatch(reader);
            }
        }

        @Override
        public void addSplitsBack(List<Assignment> splits, int reader) {
            if (!splits.isEmpty() && (splits.size() != 1 || reader != 0)) {
                throw new IllegalStateException("Invalid WebSocketServer assignment");
            }
            // The engine sends reader states before initial registration. Either an empty
            // checkpoint or a restored assignment recreates this live listener once.
        }

        @Override
        public int currentUnassignedSplitSize() {
            synchronized (context) {
                return assigned ? 0 : 1;
            }
        }

        @Override
        public void handleSplitRequest(int reader) {
            registerReader(reader);
        }

        @Override
        public Assignment snapshotState(long checkpointId) {
            return new Assignment();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void close() {}

        private void dispatch(int reader) {
            if (!running) {
                return;
            }
            if (reader == 0 && !assigned) {
                context.assignSplit(reader, new Assignment());
                assigned = true;
            } else if (reader != 0 && idle.add(reader)) {
                context.signalNoMoreSplits(reader);
            }
        }
    }

    /**
     * Decode and emit only on the task thread, separately from network queue admission. Checkpoints
     * store listener assignment only and never persist peer data.
     */
    private static final class ListenerReader implements SourceReader<SeaTunnelRow, Assignment> {
        private final Context context;
        private final WebSocketServerSourceOptions.Settings settings;
        private final CatalogTable table;
        private final WebSocketServerDecoder decoder;
        // Publication/cancel are serialized; decoding runs on the task thread outside this lock.
        private final Object lifecycle = new Object();
        private volatile boolean closed;
        private volatile boolean assigned;
        private volatile boolean idle;
        private boolean signaled;
        private volatile WebSocketServer server;

        private ListenerReader(
                Context context,
                WebSocketServerSourceOptions.Settings settings,
                CatalogTable table,
                boolean customSchema) {
            this.context = context;
            this.settings = settings;
            this.table = table;
            decoder =
                    "json".equals(settings.format) || customSchema && "text".equals(settings.format)
                            ? new WebSocketServerDecoder(
                                    table,
                                    settings.format,
                                    settings.delimiter,
                                    settings.decodedUnits)
                            : null;
        }

        @Override
        public void open() throws IOException {
            synchronized (lifecycle) {
                if (!closed && context.getIndexOfSubtask() == 0 && server == null) {
                    server = new WebSocketServer(settings);
                }
            }
        }

        @Override
        public void pollNext(Collector<SeaTunnelRow> out) throws IOException {
            if (closed) {
                return;
            }
            if (!assigned) {
                if (idle && !signaled) {
                    signaled = true;
                    context.signalNoMoreElement();
                }
                return;
            }
            WebSocketServer.Message message = server.poll();
            if (message == null || closed) {
                return;
            }
            // Dequeue/decode is not durable state. Only emission is checkpoint-lock coordinated,
            // matching the assignment-only recovery contract documented for this live receiver.
            SeaTunnelRow row = decode(message);
            if (row != null) {
                synchronized (out.getCheckpointLock()) {
                    if (!closed) {
                        out.collect(row);
                    }
                }
            }
        }

        @Override
        public List<Assignment> snapshotState(long checkpointId) {
            return assigned ? Collections.singletonList(new Assignment()) : Collections.emptyList();
        }

        @Override
        public void addSplits(List<Assignment> splits) {
            if (splits.size() != 1 || context.getIndexOfSubtask() != 0) {
                throw new IllegalStateException("Invalid WebSocketServer reader assignment");
            }
            assigned = true;
        }

        @Override
        public void handleNoMoreSplits() {
            idle = true;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void cancel() {
            synchronized (lifecycle) {
                closed = true;
                if (server != null) {
                    server.close();
                }
            }
        }

        @Override
        public void close() {
            cancel();
        }

        private SeaTunnelRow decode(WebSocketServer.Message message) throws IOException {
            try {
                SeaTunnelRow row;
                if (decoder != null) {
                    row = decoder.decode(message.data);
                } else if ("raw".equals(settings.format)) {
                    row =
                            new SeaTunnelRow(
                                    new Object[] {message.text ? "text" : "binary", message.data});
                } else {
                    row =
                            new SeaTunnelRow(
                                    new Object[] {WebSocketServerProtocol.text(message.data)});
                }
                if (row == null) {
                    throw new IOException("WebSocketServer expects one parsed row per message");
                }
                row.setTableId(table.getTablePath().getFullName());
                return row;
            } catch (IOException | RuntimeException ex) {
                throw new IOException(
                        "WebSocketServer payload exceeds decoded work budget or does not match the configured format or schema");
            }
        }
    }
}
