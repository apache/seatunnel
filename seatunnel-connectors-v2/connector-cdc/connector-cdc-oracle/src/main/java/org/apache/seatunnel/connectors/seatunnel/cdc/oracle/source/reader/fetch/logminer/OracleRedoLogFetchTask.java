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
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleConnectorConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.OracleSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleConnectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;

/** The task to work for fetching data of Oracle table stream split. */
public class OracleRedoLogFetchTask implements FetchTask<SourceSplitBase> {

    private final IncrementalSplit split;
    private volatile boolean taskRunning = false;

    public OracleRedoLogFetchTask(IncrementalSplit split) {
        this.split = split;
    }

    @Override
    public void execute(FetchTask.Context context) throws Exception {
        OracleSourceFetchTaskContext sourceFetchContext = (OracleSourceFetchTaskContext) context;
        taskRunning = true;
        OracleConnectorConfig dbzConnectorConfig = sourceFetchContext.getDbzConnectorConfig();
        try (OracleConnection oracleConnection =
                OracleConnectionUtils.createOracleConnection(
                        sourceFetchContext.getDbzConnectorConfig().getJdbcConfig())) {
            RedoLogSplitReadTask redoLogSplitReadTask =
                    new RedoLogSplitReadTask(
                            dbzConnectorConfig,
                            oracleConnection,
                            sourceFetchContext.getDispatcher(),
                            sourceFetchContext.getErrorHandler(),
                            sourceFetchContext.getDatabaseSchema(),
                            sourceFetchContext.getSourceConfig().getOriginDbzConnectorConfig(),
                            sourceFetchContext.getStreamingChangeEventSourceMetrics(),
                            split);
            RedoLogSplitChangeEventSourceContext changeEventSourceContext =
                    new RedoLogSplitChangeEventSourceContext();
            redoLogSplitReadTask.execute(
                    changeEventSourceContext,
                    sourceFetchContext.getPartition(),
                    sourceFetchContext.getOffsetContext());
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
    public IncrementalSplit getSplit() {
        return split;
    }

    /**
     * A wrapped task to read all redoLog for table and also supports read bounded (from
     * lowWatermark to highWatermark) redoLog.
     */
    public static class RedoLogSplitReadTask extends LogMinerStreamingChangeEventSource {

        private static final Logger LOG = LoggerFactory.getLogger(RedoLogSplitReadTask.class);
        private final IncrementalSplit redoLogSplit;
        private final JdbcSourceEventDispatcher<OraclePartition> dispatcher;
        private final ErrorHandler errorHandler;
        private ChangeEventSourceContext context;

        private final OracleConnectorConfig connectorConfig;
        private final OracleConnection connection;

        private final OracleDatabaseSchema schema;

        private final OracleStreamingChangeEventSourceMetrics metrics;

        public RedoLogSplitReadTask(
                OracleConnectorConfig connectorConfig,
                OracleConnection connection,
                JdbcSourceEventDispatcher<OraclePartition> dispatcher,
                ErrorHandler errorHandler,
                OracleDatabaseSchema schema,
                Configuration jdbcConfig,
                OracleStreamingChangeEventSourceMetrics metrics,
                IncrementalSplit redoLogSplit) {
            super(
                    connectorConfig,
                    connection,
                    dispatcher,
                    errorHandler,
                    Clock.SYSTEM,
                    schema,
                    jdbcConfig,
                    metrics);
            this.redoLogSplit = redoLogSplit;
            this.dispatcher = dispatcher;
            this.errorHandler = errorHandler;
            this.connectorConfig = connectorConfig;
            this.connection = connection;
            this.metrics = metrics;
            this.schema = schema;
        }

        @Override
        public void execute(
                ChangeEventSourceContext context,
                OraclePartition oraclePartition,
                OracleOffsetContext offsetContext) {
            this.context = context;
            if (connectorConfig.getPdbName() != null) {
                connection.resetSessionToCdb();
            }
            super.execute(context, oraclePartition, offsetContext);
        }

        @Override
        protected LogMinerEventProcessor createProcessor(
                ChangeEventSourceContext context,
                OraclePartition partition,
                OracleOffsetContext offsetContext) {
            return EventProcessorFactory.createProcessor(
                    context,
                    connectorConfig,
                    connection,
                    dispatcher,
                    partition,
                    offsetContext,
                    schema,
                    metrics,
                    errorHandler,
                    redoLogSplit);
        }
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for redoLog split task.
     */
    private class RedoLogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
