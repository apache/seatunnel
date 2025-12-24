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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.logminer;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.offset.RedoLogOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.scan.OracleSnapshotFetchTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.infinispan.EmbeddedInfinispanLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.infinispan.RemoteInfinispanLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.memory.MemoryLogMinerEventProcessor;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;

import java.sql.SQLException;

/**
 * Factory to produce a LogMinerEventProcessor with enhanced processRow method to distinguish
 * whether is bounded.
 */
public class EventProcessorFactory {
    private static final Logger LOG = LoggerFactory.getLogger(EventProcessorFactory.class);

    private EventProcessorFactory() {}

    public static LogMinerEventProcessor createProcessor(
            ChangeEventSource.ChangeEventSourceContext context,
            OracleConnectorConfig connectorConfig,
            OracleConnection jdbcConnection,
            JdbcSourceEventDispatcher<OraclePartition> dispatcher,
            OraclePartition partition,
            OracleOffsetContext offsetContext,
            OracleDatabaseSchema schema,
            OracleStreamingChangeEventSourceMetrics metrics,
            ErrorHandler errorHandler,
            IncrementalSplit redoLogSplit) {
        final OracleConnectorConfig.LogMiningBufferType bufferType =
                connectorConfig.getLogMiningBufferType();
        if (bufferType.equals(OracleConnectorConfig.LogMiningBufferType.MEMORY)) {
            return new CDCMemoryLogMinerEventProcessor(
                    context,
                    connectorConfig,
                    jdbcConnection,
                    dispatcher,
                    partition,
                    offsetContext,
                    schema,
                    metrics,
                    errorHandler,
                    redoLogSplit);
        } else if (bufferType.equals(
                OracleConnectorConfig.LogMiningBufferType.INFINISPAN_EMBEDDED)) {
            return new CDCEmbeddedInfinispanLogMinerEventProcessor(
                    context,
                    connectorConfig,
                    jdbcConnection,
                    dispatcher,
                    partition,
                    offsetContext,
                    schema,
                    metrics,
                    errorHandler,
                    redoLogSplit);
        } else if (bufferType.equals(OracleConnectorConfig.LogMiningBufferType.INFINISPAN_REMOTE)) {
            return new CDCRemoteInfinispanLogMinerEventProcessor(
                    context,
                    connectorConfig,
                    jdbcConnection,
                    dispatcher,
                    partition,
                    offsetContext,
                    schema,
                    metrics,
                    errorHandler,
                    redoLogSplit);
        } else {
            throw new IllegalArgumentException(
                    "not support this type of bufferType: " + bufferType);
        }
    }

    /**
     * A {@link MemoryLogMinerEventProcessor} with enhanced processRow method to distinguish whether
     * is bounded.
     */
    public static class CDCMemoryLogMinerEventProcessor extends MemoryLogMinerEventProcessor {
        private final IncrementalSplit redoLogSplit;
        private final ErrorHandler errorHandler;

        private ChangeEventSource.ChangeEventSourceContext context;
        private final JdbcSourceEventDispatcher<OraclePartition> dispatcher;

        public CDCMemoryLogMinerEventProcessor(
                ChangeEventSource.ChangeEventSourceContext context,
                OracleConnectorConfig connectorConfig,
                OracleConnection jdbcConnection,
                JdbcSourceEventDispatcher<OraclePartition> dispatcher,
                OraclePartition partition,
                OracleOffsetContext offsetContext,
                OracleDatabaseSchema schema,
                OracleStreamingChangeEventSourceMetrics metrics,
                ErrorHandler errorHandler,
                IncrementalSplit redoLogSplit) {
            super(
                    context,
                    connectorConfig,
                    jdbcConnection,
                    dispatcher,
                    partition,
                    offsetContext,
                    schema,
                    metrics);
            this.redoLogSplit = redoLogSplit;
            this.errorHandler = errorHandler;
            this.context = context;
            this.dispatcher = dispatcher;
        }

        @Override
        protected void processRow(OraclePartition partition, LogMinerEventRow row)
                throws SQLException, InterruptedException {
            if (reachEndingOffset(
                    partition, row, redoLogSplit, errorHandler, dispatcher, context)) {
                return;
            }
            super.processRow(partition, row);
        }
    }

    /**
     * A {@link EmbeddedInfinispanLogMinerEventProcessor} with enhanced processRow method to
     * distinguish whether is bounded.
     */
    public static class CDCEmbeddedInfinispanLogMinerEventProcessor
            extends EmbeddedInfinispanLogMinerEventProcessor {
        private final IncrementalSplit redoLogSplit;
        private final ErrorHandler errorHandler;

        private ChangeEventSource.ChangeEventSourceContext context;
        private final JdbcSourceEventDispatcher<OraclePartition> dispatcher;

        public CDCEmbeddedInfinispanLogMinerEventProcessor(
                ChangeEventSource.ChangeEventSourceContext context,
                OracleConnectorConfig connectorConfig,
                OracleConnection jdbcConnection,
                JdbcSourceEventDispatcher<OraclePartition> dispatcher,
                OraclePartition partition,
                OracleOffsetContext offsetContext,
                OracleDatabaseSchema schema,
                OracleStreamingChangeEventSourceMetrics metrics,
                ErrorHandler errorHandler,
                IncrementalSplit redoLogSplit) {
            super(
                    context,
                    connectorConfig,
                    jdbcConnection,
                    dispatcher,
                    partition,
                    offsetContext,
                    schema,
                    metrics);
            this.redoLogSplit = redoLogSplit;
            this.errorHandler = errorHandler;
            this.context = context;
            this.dispatcher = dispatcher;
        }

        @Override
        protected void processRow(OraclePartition partition, LogMinerEventRow row)
                throws SQLException, InterruptedException {
            if (reachEndingOffset(
                    partition, row, redoLogSplit, errorHandler, dispatcher, context)) {
                return;
            }
            super.processRow(partition, row);
        }
    }

    /**
     * A {@link CDCRemoteInfinispanLogMinerEventProcessor} with enhanced processRow method to
     * distinguish whether is bounded.
     */
    public static class CDCRemoteInfinispanLogMinerEventProcessor
            extends RemoteInfinispanLogMinerEventProcessor {
        private final IncrementalSplit redoLogSplit;
        private final ErrorHandler errorHandler;

        private ChangeEventSource.ChangeEventSourceContext context;
        private final JdbcSourceEventDispatcher<OraclePartition> dispatcher;

        public CDCRemoteInfinispanLogMinerEventProcessor(
                ChangeEventSource.ChangeEventSourceContext context,
                OracleConnectorConfig connectorConfig,
                OracleConnection jdbcConnection,
                JdbcSourceEventDispatcher<OraclePartition> dispatcher,
                OraclePartition partition,
                OracleOffsetContext offsetContext,
                OracleDatabaseSchema schema,
                OracleStreamingChangeEventSourceMetrics metrics,
                ErrorHandler errorHandler,
                IncrementalSplit redoLogSplit) {
            super(
                    context,
                    connectorConfig,
                    jdbcConnection,
                    dispatcher,
                    partition,
                    offsetContext,
                    schema,
                    metrics);
            this.redoLogSplit = redoLogSplit;
            this.errorHandler = errorHandler;
            this.context = context;
            this.dispatcher = dispatcher;
        }

        @Override
        protected void processRow(OraclePartition partition, LogMinerEventRow row)
                throws SQLException, InterruptedException {
            if (reachEndingOffset(
                    partition, row, redoLogSplit, errorHandler, dispatcher, context)) {
                return;
            }
            super.processRow(partition, row);
        }
    }

    public static boolean reachEndingOffset(
            OraclePartition partition,
            LogMinerEventRow row,
            IncrementalSplit redoLogSplit,
            ErrorHandler errorHandler,
            JdbcSourceEventDispatcher dispatcher,
            ChangeEventSource.ChangeEventSourceContext context) {
        // check do we need to stop for fetch redo log for snapshot split.
        if (isBoundedRead(redoLogSplit)) {
            final RedoLogOffset currentRedoLogOffset = new RedoLogOffset(row.getScn().longValue());
            // reach the high watermark, the redo log fetcher should be finished
            if (currentRedoLogOffset.isAtOrAfter(redoLogSplit.getStopOffset())) {
                // send redo log end event
                try {
                    dispatcher.dispatchWatermarkEvent(
                            partition.getSourcePartition(),
                            redoLogSplit,
                            currentRedoLogOffset,
                            WatermarkKind.END);
                } catch (InterruptedException e) {
                    LOG.error("Send signal event error.", e);
                    errorHandler.setProducerThrowable(
                            new DebeziumException("Error processing redo log signal event", e));
                }
                // tell fetcher the redo log task finished
                ((OracleSnapshotFetchTask.SnapshotRedoLogSplitChangeEventSourceContext) context)
                        .finished();
                return true;
            }
        }
        return false;
    }

    private static boolean isBoundedRead(IncrementalSplit redoLogSplit) {
        return !RedoLogOffset.NO_STOPPING_OFFSET.equals(redoLogSplit.getStopOffset());
    }
}
