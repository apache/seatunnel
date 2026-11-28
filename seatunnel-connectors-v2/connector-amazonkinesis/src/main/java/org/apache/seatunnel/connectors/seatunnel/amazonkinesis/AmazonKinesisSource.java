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

package org.apache.seatunnel.connectors.seatunnel.amazonkinesis;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.amazonkinesis.KinesisConnection.Settings.bounded;

/**
 * Reads one Kinesis stream using per-shard checkpoints and one active reader per source instance.
 */
public final class AmazonKinesisSource
        implements SeaTunnelSource<SeaTunnelRow, KinesisSplit, KinesisSplit> {
    static final String IDENTIFIER = "AmazonKinesis";
    private static final long serialVersionUID = 1L;
    private final KinesisConnection.Settings connection;
    private final ReadSettings settings;
    private boolean streaming;
    private boolean checkpointing;

    public AmazonKinesisSource(ReadonlyConfig options) {
        connection = new KinesisConnection.Settings(options);
        settings = new ReadSettings(options);
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public void setJobContext(JobContext context) {
        streaming = JobMode.STREAMING.equals(context.getJobMode());
        checkpointing = context.isEnableCheckpoint();
    }

    @Override
    public Boundedness getBoundedness() {
        return streaming ? Boundedness.UNBOUNDED : Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        TableSchema schema =
                TableSchema.builder()
                        .column(column("stream_arn", BasicType.STRING_TYPE, false))
                        .column(column("shard_id", BasicType.STRING_TYPE, false))
                        .column(column("sequence_number", BasicType.STRING_TYPE, false))
                        .column(column("partition_key", BasicType.STRING_TYPE, true))
                        .column(column("arrival_timestamp", BasicType.STRING_TYPE, true))
                        .column(column("data", PrimitiveByteArrayType.INSTANCE, false))
                        .build();
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, TablePath.DEFAULT),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null));
    }

    private static PhysicalColumn column(String name, SeaTunnelDataType<?> type, boolean nullable) {
        return PhysicalColumn.of(name, type, 0, nullable, null, null);
    }

    @Override
    public SourceReader<SeaTunnelRow, KinesisSplit> createReader(SourceReader.Context context) {
        return new KinesisReader(connection, settings, context, streaming, checkpointing, scope());
    }

    @Override
    public SourceSplitEnumerator<KinesisSplit, KinesisSplit> createEnumerator(
            SourceSplitEnumerator.Context<KinesisSplit> context) {
        Instant anchor = "LATEST".equals(settings.start) ? Instant.now() : settings.timestamp;
        return new Enumerator(context, scope(), new KinesisSplit(scope(), anchor));
    }

    @Override
    public SourceSplitEnumerator<KinesisSplit, KinesisSplit> restoreEnumerator(
            SourceSplitEnumerator.Context<KinesisSplit> context, KinesisSplit checkpoint) {
        checkpoint.verify(scope());
        return new Enumerator(context, scope(), checkpoint);
    }

    private String scope() {
        return connection.scope()
                + "/"
                + streaming
                + "/"
                + settings.start
                + "/"
                + settings.timestamp;
    }

    static final class ReadSettings implements Serializable {
        private static final long serialVersionUID = 1L;
        final String start;
        final Instant timestamp;
        final int records;
        final int shardPollMs;
        final int discoveryMs;
        final int batchTimeout;

        ReadSettings(ReadonlyConfig options) {
            start = options.get(AmazonKinesisSourceOptions.START_POSITION);
            String configured = options.get(AmazonKinesisSourceOptions.START_TIMESTAMP);
            String value = configured == null || configured.isEmpty() ? null : configured;
            if (!"TRIM_HORIZON".equals(start)
                            && !"LATEST".equals(start)
                            && !"AT_TIMESTAMP".equals(start)
                    || ("AT_TIMESTAMP".equals(start) != (value != null))) {
                throw new IllegalArgumentException(
                        "Kinesis start_timestamp must be supplied only for AT_TIMESTAMP");
            }
            try {
                timestamp = value == null ? null : Instant.parse(value);
            } catch (DateTimeException ex) {
                throw new IllegalArgumentException(
                        "Kinesis start_timestamp must be an ISO-8601 instant");
            }
            records = bounded(options, AmazonKinesisSourceOptions.RECORD_LIMIT, 1, 10000);
            shardPollMs = bounded(options, AmazonKinesisSourceOptions.SHARD_POLL_MS, 200, 60000);
            discoveryMs = bounded(options, AmazonKinesisSourceOptions.DISCOVERY_MS, 1000, 600000);
            batchTimeout =
                    bounded(options, AmazonKinesisSourceOptions.BATCH_TIMEOUT_MS, 1000, 86400000);
        }
    }

    /** Stores the initial anchor even if the first checkpoint precedes reader assignment. */
    private static final class Enumerator
            implements SourceSplitEnumerator<KinesisSplit, KinesisSplit> {
        private final Context<KinesisSplit> context;
        private final String scope;
        private final Set<Integer> finishedIdleReaders = new HashSet<>();
        private KinesisSplit pending;
        private boolean running;
        private boolean assigned;

        Enumerator(Context<KinesisSplit> context, String scope, KinesisSplit pending) {
            this.context = context;
            this.scope = scope;
            this.pending = pending.copy();
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

        private void dispatch(int subtask) {
            if (!running) {
                return;
            }
            if (subtask == 0 && !assigned) {
                context.assignSplit(subtask, pending.copy());
                assigned = true;
            } else if (subtask != 0 && finishedIdleReaders.add(subtask)) {
                context.signalNoMoreSplits(subtask);
            }
        }

        @Override
        public void addSplitsBack(List<KinesisSplit> splits, int subtask) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || subtask != 0) {
                throw new IllegalStateException("Invalid Kinesis restored assignment");
            }
            synchronized (context) {
                KinesisSplit restored = splits.get(0);
                restored.verify(scope);
                pending = restored.copy();
                assigned = false;
                if (context.registeredReaders().contains(0)) {
                    dispatch(0);
                }
            }
        }

        @Override
        public int currentUnassignedSplitSize() {
            synchronized (context) {
                return assigned ? 0 : 1;
            }
        }

        @Override
        public void registerReader(int subtask) {
            synchronized (context) {
                dispatch(subtask);
            }
        }

        @Override
        public void handleSplitRequest(int subtask) {
            registerReader(subtask);
        }

        @Override
        public KinesisSplit snapshotState(long checkpoint) {
            synchronized (context) {
                return pending.copy();
            }
        }

        @Override
        public void close() {}

        @Override
        public void notifyCheckpointComplete(long checkpoint) {}
    }
}
