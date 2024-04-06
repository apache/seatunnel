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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.scan;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleConnectorConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.OracleSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.logminer.OracleRedoLogFetchTask;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.logminer.LogMinerOracleOffsetContextLoader;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

/** The task to work for fetching data of Oracle table snapshot split. */
@Slf4j
public class OracleSnapshotFetchTask implements FetchTask<SourceSplitBase> {

    private final SnapshotSplit split;
    private volatile boolean taskRunning = false;

    private OracleSnapshotSplitReadTask snapshotSplitReadTask;

    public OracleSnapshotFetchTask(SnapshotSplit split) {
        this.split = split;
    }

    @Override
    public SnapshotSplit getSplit() {
        return split;
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
    public void execute(Context context) throws Exception {
        OracleSourceFetchTaskContext sourceFetchContext = (OracleSourceFetchTaskContext) context;
        taskRunning = true;
        snapshotSplitReadTask =
                new OracleSnapshotSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getSnapshotChangeEventSourceMetrics(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getDispatcher(),
                        split);
        SnapshotSplitChangeEventSourceContext changeEventSourceContext =
                new SnapshotSplitChangeEventSourceContext();
        SnapshotResult<OracleOffsetContext> snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext,
                        sourceFetchContext.getPartition(),
                        sourceFetchContext.getOffsetContext());
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
                log.debug("Skip merge changelog(exactly-once) for snapshot split {}", split);
            }
            return;
        }

        final IncrementalSplit backfillSplit = createBackfillRedoLogSplit(changeEventSourceContext);
        // optimization that skip the redoLog read when the low watermark equals high
        // watermark
        if (!changed) {
            dispatchRedoLogEndEvent(
                    backfillSplit,
                    sourceFetchContext.getPartition().getSourcePartition(),
                    sourceFetchContext.getDispatcher());
            taskRunning = false;
            return;
        }
        // execute redoLog read task
        final OracleRedoLogFetchTask.RedoLogSplitReadTask backfillReadTask =
                createBackfillRedoLogReadTask(backfillSplit, sourceFetchContext);

        OracleConnectorConfig oracleConnectorConfig =
                sourceFetchContext.getSourceConfig().getDbzConnectorConfig();
        final OffsetContext.Loader<OracleOffsetContext> loader =
                new LogMinerOracleOffsetContextLoader(oracleConnectorConfig);
        final OracleOffsetContext oracleOffsetContext =
                loader.load(backfillSplit.getStartupOffset().getOffset());
        log.info(
                "start execute backfillReadTask, start offset : {}, stop offset : {}",
                backfillSplit.getStartupOffset(),
                backfillSplit.getStopOffset());
        backfillReadTask.execute(
                new SnapshotRedoLogSplitChangeEventSourceContext(),
                sourceFetchContext.getPartition(),
                oracleOffsetContext);
        log.info("backfillReadTask execute end");

        taskRunning = false;
    }

    private IncrementalSplit createBackfillRedoLogSplit(
            SnapshotSplitChangeEventSourceContext sourceContext) {
        return new IncrementalSplit(
                split.splitId(),
                Collections.singletonList(split.getTableId()),
                sourceContext.getLowWatermark(),
                sourceContext.getHighWatermark(),
                new ArrayList<>());
    }

    private OracleRedoLogFetchTask.RedoLogSplitReadTask createBackfillRedoLogReadTask(
            IncrementalSplit backfillRedoLogSplit, OracleSourceFetchTaskContext context) {
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
        // task to read redoLog and backfill for current split
        return new OracleRedoLogFetchTask.RedoLogSplitReadTask(
                new OracleConnectorConfig(dezConf),
                context.getConnection(),
                context.getDispatcher(),
                context.getErrorHandler(),
                context.getDatabaseSchema(),
                context.getSourceConfig().getOriginDbzConnectorConfig(),
                context.getStreamingChangeEventSourceMetrics(),
                backfillRedoLogSplit);
    }

    private void dispatchRedoLogEndEvent(
            IncrementalSplit backFillRedoLogSplit,
            Map<String, ?> sourcePartition,
            JdbcSourceEventDispatcher<OraclePartition> eventDispatcher)
            throws InterruptedException {
        eventDispatcher.dispatchWatermarkEvent(
                sourcePartition,
                backFillRedoLogSplit,
                backFillRedoLogSplit.getStopOffset(),
                WatermarkKind.END);
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
