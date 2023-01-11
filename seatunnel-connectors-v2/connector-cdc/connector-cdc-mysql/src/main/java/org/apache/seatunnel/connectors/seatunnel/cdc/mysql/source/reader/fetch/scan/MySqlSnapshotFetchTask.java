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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch.scan;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils.createMySqlConnection;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch.MySqlSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch.binlog.MySqlBinlogFetchTask;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

public class MySqlSnapshotFetchTask implements FetchTask<SourceSplitBase> {

    private final SnapshotSplit split;

    private volatile boolean taskRunning = false;

    private MySqlSnapshotSplitReadTask snapshotSplitReadTask;

    public MySqlSnapshotFetchTask(SnapshotSplit split) {
        this.split = split;
    }

    @Override
    public void execute(FetchTask.Context context) throws Exception {
        MySqlSourceFetchTaskContext sourceFetchContext = (MySqlSourceFetchTaskContext) context;
        taskRunning = true;
        snapshotSplitReadTask =
            new MySqlSnapshotSplitReadTask(
                sourceFetchContext.getDbzConnectorConfig(),
                sourceFetchContext.getOffsetContext(),
                sourceFetchContext.getSnapshotChangeEventSourceMetrics(),
                sourceFetchContext.getDatabaseSchema(),
                sourceFetchContext.getConnection(),
                sourceFetchContext.getDispatcher(),
                split);
        SnapshotSplitChangeEventSourceContext changeEventSourceContext =
            new SnapshotSplitChangeEventSourceContext();
        SnapshotResult snapshotResult = snapshotSplitReadTask.execute(
                changeEventSourceContext, sourceFetchContext.getOffsetContext());

        final IncrementalSplit backfillBinlogSplit = createBackfillBinlogSplit(changeEventSourceContext);

        // optimization that skip the binlog read when the low watermark equals high
        // watermark
        final boolean binlogBackfillRequired =
            backfillBinlogSplit
                .getStopOffset()
                .isAfter(backfillBinlogSplit.getStartupOffset());
        if (!binlogBackfillRequired) {
            dispatchBinlogEndEvent(
                backfillBinlogSplit,
                ((MySqlSourceFetchTaskContext) context).getOffsetContext().getPartition(),
                ((MySqlSourceFetchTaskContext) context).getDispatcher());
            taskRunning = false;
            return;
        }
        // execute binlog read task
        if (snapshotResult.isCompletedOrSkipped()) {
            final MySqlBinlogFetchTask.MySqlBinlogSplitReadTask backfillBinlogReadTask =
                createBackfillBinlogReadTask(backfillBinlogSplit, sourceFetchContext);
            backfillBinlogReadTask.execute(
                new SnapshotBinlogSplitChangeEventSourceContext(),
                sourceFetchContext.getOffsetContext());
        } else {
            taskRunning = false;
            throw new IllegalStateException(
                String.format("Read snapshot for mysql split %s fail", split));
        }
    }

    private IncrementalSplit createBackfillBinlogSplit(
        SnapshotSplitChangeEventSourceContext sourceContext) {
        return new IncrementalSplit(
            split.splitId(),
            Collections.singletonList(split.getTableId()),
            sourceContext.getLowWatermark(),
            sourceContext.getHighWatermark(),
            new ArrayList<>());
    }

    private void dispatchBinlogEndEvent(
        IncrementalSplit backFillBinlogSplit,
        Map<String, ?> sourcePartition,
        JdbcSourceEventDispatcher eventDispatcher)
        throws InterruptedException {
        eventDispatcher.dispatchWatermarkEvent(
            sourcePartition,
            backFillBinlogSplit,
            backFillBinlogSplit.getStopOffset(),
            WatermarkKind.END);
    }

    private MySqlBinlogFetchTask.MySqlBinlogSplitReadTask createBackfillBinlogReadTask(
        IncrementalSplit backfillBinlogSplit, MySqlSourceFetchTaskContext context) {
        final MySqlOffsetContext.Loader loader =
            new MySqlOffsetContext.Loader(context.getSourceConfig().getDbzConnectorConfig());
        final MySqlOffsetContext mySqlOffsetContext =
            (MySqlOffsetContext)
                loader.load(backfillBinlogSplit.getStartupOffset().getOffset());
        // we should only capture events for the current table,
        // otherwise, we may can't find corresponding schema
        Configuration dezConf =
            context.getSourceConfig()
                .getDbzConfiguration()
                .edit()
                .with("table.include.list", split.getTableId().toString())
                // Disable heartbeat event in snapshot split fetcher
                .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                .build();
        // task to read binlog and backfill for current split
        return new MySqlBinlogFetchTask.MySqlBinlogSplitReadTask(
            new MySqlConnectorConfig(dezConf),
            mySqlOffsetContext,
            createMySqlConnection(context.getSourceConfig().getDbzConfiguration()),
            context.getDispatcher(),
            context.getErrorHandler(),
            context.getTaskContext(),
            context.getStreamingChangeEventSourceMetrics(),
            backfillBinlogSplit);
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for bounded binlog task
     * of a snapshot split task.
     */
    public class SnapshotBinlogSplitChangeEventSourceContext
        implements ChangeEventSource.ChangeEventSourceContext {

        public void finished() {
            taskRunning = false;
        }

        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
