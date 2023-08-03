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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.source.reader.fetch.scan;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.source.reader.fetch.OracleSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.source.reader.fetch.redolog.OracleRedoLogFetchTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.logminer.LogMinerOracleOffsetContextLoader;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

public class OracleSnapshotFetchTask implements FetchTask<SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(OracleSnapshotFetchTask.class);
    private final SnapshotSplit split;

    private volatile boolean taskRunning = false;

    private OracleSnapshotSplitReadTask snapshotSplitReadTask;

    public OracleSnapshotFetchTask(SnapshotSplit snapshotSplit) {
        this.split = snapshotSplit;
    }

    @Override
    public void execute(Context context) throws Exception {
        OracleSourceFetchTaskContext sourceFetchContext = (OracleSourceFetchTaskContext) context;
        taskRunning = true;
        snapshotSplitReadTask =
                new OracleSnapshotSplitReadTask(
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
                    String.format("Read snapshot for oracle split %s fail", split));
        }

        boolean changed =
                changeEventSourceContext
                        .getHighWatermark()
                        .isAfter(changeEventSourceContext.getLowWatermark());
        if (!context.isExactlyOnce()) {
            taskRunning = false;
            if (changed) {
                LOG.debug("Skip merge changelog(exactly-once) for snapshot split {}", split);
            }
            return;
        }
        IncrementalSplit backFillRedoLogSplit =
                createBackFillRedoLogSplit(changeEventSourceContext);
        // optimization that skip the RedoLog read when the low watermark equals high
        // watermark
        if (!changed) {
            dispatchRedoLogEndEvent(
                    backFillRedoLogSplit,
                    ((OracleSourceFetchTaskContext) context).getOffsetContext().getPartition(),
                    ((OracleSourceFetchTaskContext) context).getDispatcher());
            taskRunning = false;
            return;
        }

        // execute redoLog read task
        final OracleRedoLogFetchTask.RedoLogSplitReadTask backfillRedoLogReadTask =
                createBackfillRedoLogReadTask(backFillRedoLogSplit, sourceFetchContext);
        final LogMinerOracleOffsetContextLoader loader =
                new LogMinerOracleOffsetContextLoader(
                        ((OracleSourceFetchTaskContext) context).getDbzConnectorConfig());
        final OracleOffsetContext oracleOffsetContext =
                loader.load(backFillRedoLogSplit.getStartupOffset().getOffset());
        LOG.info(
                "start execute backfillReadTask, start offset : {}, stop offset : {}",
                backFillRedoLogSplit.getStartupOffset(),
                backFillRedoLogSplit.getStopOffset());
        backfillRedoLogReadTask.execute(
                new SnapshotRedoLogSplitChangeEventSourceContext(), oracleOffsetContext);
        LOG.info("backfillReadTask execute end");

        taskRunning = false;
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

    private IncrementalSplit createBackFillRedoLogSplit(
            SnapshotSplitChangeEventSourceContext sourceContext) {
        return new IncrementalSplit(
                split.splitId(),
                Collections.singletonList(split.getTableId()),
                sourceContext.getLowWatermark(),
                sourceContext.getHighWatermark(),
                new ArrayList<>());
    }

    private void dispatchRedoLogEndEvent(
            IncrementalSplit backFillRedoLogSplit,
            Map<String, ?> sourcePartition,
            JdbcSourceEventDispatcher eventDispatcher)
            throws InterruptedException {
        eventDispatcher.dispatchWatermarkEvent(
                sourcePartition,
                backFillRedoLogSplit,
                backFillRedoLogSplit.getStopOffset(),
                WatermarkKind.END);
    }

    private OracleRedoLogFetchTask.RedoLogSplitReadTask createBackfillRedoLogReadTask(
            IncrementalSplit backFillRedoLogSplit, OracleSourceFetchTaskContext context) {
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
        return new OracleRedoLogFetchTask.RedoLogSplitReadTask(
                new OracleConnectorConfig(dezConf),
                context.getDataConnection(),
                context.getDispatcher(),
                context.getErrorHandler(),
                context.getDatabaseSchema(),
                context.getSourceConfig().getOriginDbzConnectorConfig(),
                context.getStreamingChangeEventSourceMetrics(),
                backFillRedoLogSplit);
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for bounded stream task
     * of a snapshot split task.
     */
    public class SnapshotRedoLogSplitChangeEventSourceContext
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
