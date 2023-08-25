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

package org.apache.searunnel.connectors.cdc.db2.source.reader.fetch.transactionlog;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.apache.searunnel.connectors.cdc.db2.source.offset.LsnOffset;
import org.apache.searunnel.connectors.cdc.db2.source.reader.fetch.Db2SourceFetchTaskContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.connector.db2.Db2DatabaseSchema;
import io.debezium.connector.db2.Db2OffsetContext;
import io.debezium.connector.db2.Db2StreamingChangeEventSource;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;

public class Db2TransactionLogFetchTask implements FetchTask<SourceSplitBase> {
    private final IncrementalSplit split;
    private volatile boolean taskRunning = false;

    public Db2TransactionLogFetchTask(IncrementalSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        Db2SourceFetchTaskContext sourceFetchContext = (Db2SourceFetchTaskContext) context;
        taskRunning = true;

        TransactionLogSplitReadTask transactionLogSplitReadTask =
                new TransactionLogSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getDataConnection(),
                        sourceFetchContext.getMetadataConnection(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getDatabaseSchema(),
                        split);

        TransactionLogSplitChangeEventSourceContext changeEventSourceContext =
                new TransactionLogSplitChangeEventSourceContext();

        transactionLogSplitReadTask.execute(
                changeEventSourceContext, sourceFetchContext.getOffsetContext());
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
     * A wrapped task to read all binlog for table and also supports read bounded (from lowWatermark
     * to highWatermark) binlog.
     */
    public static class TransactionLogSplitReadTask extends Db2StreamingChangeEventSource {

        private static final Logger LOG =
                LoggerFactory.getLogger(TransactionLogSplitReadTask.class);
        private final IncrementalSplit lsnSplit;
        private final JdbcSourceEventDispatcher dispatcher;
        private final ErrorHandler errorHandler;
        private ChangeEventSource.ChangeEventSourceContext context;

        public TransactionLogSplitReadTask(
                Db2ConnectorConfig connectorConfig,
                Db2Connection connection,
                Db2Connection metadataConnection,
                JdbcSourceEventDispatcher dispatcher,
                ErrorHandler errorHandler,
                Db2DatabaseSchema schema,
                IncrementalSplit lsnSplit) {
            super(
                    connectorConfig,
                    connection,
                    metadataConnection,
                    dispatcher,
                    errorHandler,
                    Clock.system(),
                    schema);
            this.lsnSplit = lsnSplit;
            this.dispatcher = dispatcher;
            this.errorHandler = errorHandler;
        }

        private boolean isBoundedRead() {
            return !LsnOffset.NO_STOPPING_OFFSET.equals(lsnSplit.getStopOffset());
        }

        @Override
        public void execute(
                ChangeEventSource.ChangeEventSourceContext context, Db2OffsetContext offsetContext)
                throws InterruptedException {
            this.context = context;
            super.execute(context, offsetContext);
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
