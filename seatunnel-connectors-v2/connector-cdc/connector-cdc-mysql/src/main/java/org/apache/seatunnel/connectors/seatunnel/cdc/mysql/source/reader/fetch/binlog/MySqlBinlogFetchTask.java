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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch.binlog;

import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch.MySqlSourceFetchTaskContext;

import io.debezium.connector.mysql.MySqlStreamingChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;

public class MySqlBinlogFetchTask implements FetchTask<SourceSplitBase> {
    private final IncrementalSplit split;
    private volatile boolean taskRunning = false;

    public MySqlBinlogFetchTask(IncrementalSplit split) {
        this.split = split;
    }

    @Override
    public void execute(FetchTask.Context context) throws Exception {
        MySqlSourceFetchTaskContext sourceFetchContext = (MySqlSourceFetchTaskContext) context;
        taskRunning = true;

        MySqlStreamingChangeEventSource mySqlStreamingChangeEventSource = new MySqlStreamingChangeEventSource(
            sourceFetchContext.getDbzConnectorConfig(),
            sourceFetchContext.getConnection(),
            sourceFetchContext.getDispatcher(),
            sourceFetchContext.getErrorHandler(),
            Clock.SYSTEM,
            sourceFetchContext.getTaskContext(),
            sourceFetchContext.getStreamingChangeEventSourceMetrics());

        BinlogSplitChangeEventSourceContext changeEventSourceContext =
            new BinlogSplitChangeEventSourceContext();

        mySqlStreamingChangeEventSource.execute(changeEventSourceContext, sourceFetchContext.getOffsetContext());
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    private class BinlogSplitChangeEventSourceContext
        implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
