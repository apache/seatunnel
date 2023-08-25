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

package org.apache.searunnel.connectors.cdc.db2.source.reader.fetch.scan;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;

import org.apache.searunnel.connectors.cdc.db2.source.reader.fetch.Db2SourceFetchTaskContext;
import org.apache.searunnel.connectors.cdc.db2.source.reader.fetch.transactionlog.Db2TransactionLogFetchTask;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.connector.db2.Db2OffsetContext;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

@Slf4j
public class Db2SnapshotFetchTask implements FetchTask<SourceSplitBase> {

    private final SnapshotSplit split;

    private volatile boolean taskRunning = false;

    private Db2SnapshotSplitReadTask snapshotSplitReadTask;

    public Db2SnapshotFetchTask(SnapshotSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        Db2SourceFetchTaskContext sourceFetchContext = (Db2SourceFetchTaskContext) context;
        taskRunning = true;
        snapshotSplitReadTask =
                new Db2SnapshotSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getSnapshotChangeEventSourceMetrics(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getDataConnection(),
                        sourceFetchContext.getDispatcher(),
                        split);
        SnapshotSplitChangeEventSourceContext changeEventSourceContext =
                new SnapshotSplitChangeEventSourceContext();

        SnapshotResult snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext, sourceFetchContext.getOffsetContext());
        if (!snapshotResult.isCompletedOrSkipped()) {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for split %s fail", split));
        }

        boolean changed =
                changeEventSourceContext
                        .getHighWatermark()
                        .isAfter(changeEventSourceContext.getLowWatermark());
        if (!context.isExactlyOnce()) {
            taskRunning = false;
            if (changed) {
                log.debug("Skip merge changelog(exactly-once) for snapshot split {}", split);
            }
            return;
        }

        final IncrementalSplit backfillSplit = createBackFillLsnSplit(changeEventSourceContext);
        // optimization that skip the binlog read when the low watermark equals high
        // watermark
        if (!changed) {
            dispatchLsnEndEvent(
                    backfillSplit,
                    ((Db2SourceFetchTaskContext) context).getOffsetContext().getPartition(),
                    ((Db2SourceFetchTaskContext) context).getDispatcher());
            taskRunning = false;
            return;
        }

        // execute stream read task
        final Db2TransactionLogFetchTask.TransactionLogSplitReadTask backfillReadTask =
                createBackFillLsnSplitReadTask(backfillSplit, sourceFetchContext);
        Db2OffsetContext db2OffsetContext =
                new Db2OffsetContext.Loader(sourceFetchContext.getDbzConnectorConfig())
                        .load(backfillSplit.getStartupOffset().getOffset());
        log.info(
                "start execute backfillReadTask, start offset : {}, stop offset : {}",
                backfillSplit.getStartupOffset(),
                backfillSplit.getStopOffset());
        backfillReadTask.execute(
                new SnapshotBinlogSplitChangeEventSourceContext(), db2OffsetContext);
        log.info("backfillReadTask execute end");
    }

    private IncrementalSplit createBackFillLsnSplit(
            SnapshotSplitChangeEventSourceContext sourceContext) {
        return new IncrementalSplit(
                split.splitId(),
                Collections.singletonList(split.getTableId()),
                sourceContext.getLowWatermark(),
                sourceContext.getHighWatermark(),
                new ArrayList<>());
    }

    private Db2TransactionLogFetchTask.TransactionLogSplitReadTask createBackFillLsnSplitReadTask(
            IncrementalSplit backfillBinlogSplit, Db2SourceFetchTaskContext context) {
        // we should only capture events for the current table,
        // otherwise, we may can't find corresponding schema
        Configuration dezConf =
                context.getSourceConfig()
                        .getDbzConfiguration()
                        .edit()
                        .with(
                                "table.include.list",
                                split.getTableId()
                                        .toString()
                                        .substring(split.getTableId().toString().indexOf(".") + 1))
                        // Disable heartbeat event in snapshot split fetcher
                        .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                        .build();
        // task to read binlog and backfill for current split
        return new Db2TransactionLogFetchTask.TransactionLogSplitReadTask(
                new Db2ConnectorConfig(dezConf),
                context.getDataConnection(),
                context.getMetadataConnection(),
                context.getDispatcher(),
                context.getErrorHandler(),
                context.getDatabaseSchema(),
                backfillBinlogSplit);
    }

    private void dispatchLsnEndEvent(
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
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for bounded stream task
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
