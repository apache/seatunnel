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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.wal;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.PostgresSourceFetchTaskContext;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresErrorHandler;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class PostgresWalFetchTask implements FetchTask<SourceSplitBase> {
    private final IncrementalSplit split;
    private volatile boolean taskRunning = false;
    private Long lastCommitLsn;
    private PostgresStreamingChangeEventSource streamingChangeEventSource;
    private PostgresOffsetContext offsetContext;

    public PostgresWalFetchTask(IncrementalSplit split) {
        this.split = split;
    }

    @Override
    public void execute(FetchTask.Context context) throws Exception {
        PostgresSourceFetchTaskContext sourceFetchContext =
                (PostgresSourceFetchTaskContext) context;
        taskRunning = true;

        streamingChangeEventSource =
                new PostgresStreamingChangeEventSource(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getSnapshotter(),
                        sourceFetchContext.getDataConnection(),
                        sourceFetchContext.getPgEventDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        Clock.SYSTEM,
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getTaskContext(),
                        sourceFetchContext.getReplicationConnection());

        offsetContext = sourceFetchContext.getOffsetContext();

        TransactionLogSplitChangeEventSourceContext changeEventSourceContext =
                new TransactionLogSplitChangeEventSourceContext();

        log.info(
                "Start streaming change event source for postgres wal split: {}",
                split.getStartupOffset().toString());
        streamingChangeEventSource.execute(
                changeEventSourceContext, sourceFetchContext.getPartition(), offsetContext);
    }

    public void commitCurrentOffset(LsnOffset offset) {
        if (streamingChangeEventSource != null && offset != null) {

            // only extracting and storing the lsn of the last commit
            Long commitLsn = offset.getLsn().asLong();
            if (commitLsn != null
                    && (lastCommitLsn == null
                            || Lsn.valueOf(commitLsn).compareTo(Lsn.valueOf(lastCommitLsn)) > 0)) {
                lastCommitLsn = commitLsn;

                Map<String, Object> offsets = new HashMap<>();
                offsets.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, lastCommitLsn);
                log.info("Committing offset {} for {}", Lsn.valueOf(lastCommitLsn), split);
                streamingChangeEventSource.commitOffset(offsets);
            }
        }
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void shutdown() {
        taskRunning = false;
    }

    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    /**
     * A bounded WAL reader used to reconcile changes made while a snapshot split is scanned.
     *
     * <p>The reader emits an END watermark after completely processing the high watermark.
     */
    public static class PostgresWalSplitReadTask extends PostgresStreamingChangeEventSource {

        /**
         * Defines the inclusive low watermark and inclusive high watermark for this reader.
         *
         * <p>The Debezium stopping LSN is populated from this split.
         */
        private final IncrementalSplit walSplit;

        /**
         * Emits the END watermark into the same queue as snapshot and WAL records.
         *
         * <p>The scan fetcher uses that watermark to finish its reconciliation buffer.
         */
        private final JdbcSourceEventDispatcher<PostgresPartition> dispatcher;

        /**
         * Propagates failures captured internally by Debezium's streaming source.
         *
         * <p>Debezium records producer failures instead of throwing them directly.
         */
        private final PostgresErrorHandler errorHandler;

        /**
         * Creates a WAL reader that stops at the snapshot split's high watermark.
         *
         * <p>The supplied replication connection belongs only to the current snapshot reader.
         */
        public PostgresWalSplitReadTask(
                PostgresConnectorConfig connectorConfig,
                Snapshotter snapshotter,
                PostgresConnection connection,
                PostgresEventDispatcher<TableId> pgEventDispatcher,
                JdbcSourceEventDispatcher<PostgresPartition> dispatcher,
                PostgresErrorHandler errorHandler,
                PostgresSchema schema,
                PostgresTaskContext taskContext,
                ReplicationConnection replicationConnection,
                IncrementalSplit walSplit) {
            super(
                    connectorConfig,
                    snapshotter,
                    connection,
                    pgEventDispatcher,
                    errorHandler,
                    Clock.SYSTEM,
                    schema,
                    taskContext,
                    replicationConnection);
            this.walSplit = walSplit;
            this.dispatcher = dispatcher;
            this.errorHandler = errorHandler;
        }

        /**
         * Reads from the low watermark until Debezium completely processes the high watermark, then
         * emits END so the snapshot fetcher can merge its buffered records.
         */
        @Override
        public void execute(
                ChangeEventSourceContext context,
                PostgresPartition partition,
                PostgresOffsetContext offsetContext)
                throws InterruptedException {
            offsetContext.setStreamingStoppingLsn(((LsnOffset) walSplit.getStopOffset()).getLsn());
            super.execute(context, partition, offsetContext);

            Throwable producerThrowable = errorHandler.getProducerThrowable();
            if (producerThrowable != null) {
                throw new DebeziumException(
                        "Failed to read the bounded PostgreSQL WAL split", producerThrowable);
            }
            if (context.isRunning()) {
                dispatcher.dispatchWatermarkEvent(
                        partition.getSourcePartition(),
                        walSplit,
                        walSplit.getStopOffset(),
                        WatermarkKind.END);
            }
        }
    }

    private class TransactionLogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
