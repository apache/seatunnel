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

package org.apache.seatunnel.connectors.cdc.dameng.source.reader.fetch.snapshot;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.cdc.dameng.source.reader.fetch.DamengSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.dameng.source.reader.fetch.logminer.DamengLogMinerSplitReadTask;

import io.debezium.connector.dameng.DamengOffsetContext;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

@RequiredArgsConstructor
public class DamengSnapshotFetchTask implements FetchTask<SourceSplitBase> {
    private final SnapshotSplit split;
    private volatile boolean taskRunning = false;
    private DamengSnapshotSplitReadTask snapshotSplitReadTask;

    @Override
    public void execute(Context context) throws Exception {
        taskRunning = true;

        DamengSourceFetchTaskContext sourceFetchContext = (DamengSourceFetchTaskContext) context;
        snapshotSplitReadTask =
                new DamengSnapshotSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getSnapshotChangeEventSourceMetrics(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getDispatcher(),
                        split);
        DamengSnapshotSplitChangeEventSourceContext changeEventSourceContext =
                new DamengSnapshotSplitChangeEventSourceContext();
        SnapshotResult snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext, sourceFetchContext.getOffsetContext());
        IncrementalSplit backfillLogMinerSplit =
                createBackfillLogMinerSplit(changeEventSourceContext);

        // optimization that skip the logminer read when the low watermark equals high watermark
        boolean logMinerBackfillRequired =
                backfillLogMinerSplit
                        .getStopOffset()
                        .isAfter(backfillLogMinerSplit.getStartupOffset());
        if (!logMinerBackfillRequired) {
            dispatchLogMinerEndEvent(
                    backfillLogMinerSplit,
                    ((DamengSourceFetchTaskContext) context).getOffsetContext().getPartition(),
                    ((DamengSourceFetchTaskContext) context).getDispatcher());
            taskRunning = false;
            return;
        }
        // execute logminer read task
        if (snapshotResult.isCompletedOrSkipped()) {
            DamengLogMinerSplitReadTask backfillLogMinerReadTask =
                    createBackfillLogMinerReadTask(backfillLogMinerSplit, sourceFetchContext);
            backfillLogMinerReadTask.execute(
                    new SnapshotScnSplitChangeEventSourceContext(),
                    sourceFetchContext.getOffsetContext());
        } else {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for dameng split %s fail", split));
        }
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public SnapshotSplit getSplit() {
        return split;
    }

    private DamengLogMinerSplitReadTask createBackfillLogMinerReadTask(
            IncrementalSplit backfillLogMinerSplit, DamengSourceFetchTaskContext context) {
        DamengOffsetContext.Loader loader =
                new DamengOffsetContext.Loader(context.getSourceConfig().getDbzConnectorConfig());
        DamengOffsetContext damengOffsetContext =
                loader.load(backfillLogMinerSplit.getStartupOffset().getOffset());
        // task to read logminer and backfill for current split
        return new DamengLogMinerSplitReadTask(
                damengOffsetContext,
                context.getSourceConfig(),
                context.getConnection(),
                context.getDispatcher(),
                context.getErrorHandler(),
                context.getDatabaseSchema(),
                backfillLogMinerSplit);
    }

    private IncrementalSplit createBackfillLogMinerSplit(
            DamengSnapshotSplitChangeEventSourceContext sourceContext) {
        return new IncrementalSplit(
                split.splitId(),
                Collections.singletonList(split.getTableId()),
                sourceContext.getLowWatermark(),
                sourceContext.getHighWatermark(),
                new ArrayList<>());
    }

    private void dispatchLogMinerEndEvent(
            IncrementalSplit backFillLogMinerSplit,
            Map<String, ?> sourcePartition,
            JdbcSourceEventDispatcher eventDispatcher)
            throws InterruptedException {
        eventDispatcher.dispatchWatermarkEvent(
                sourcePartition,
                backFillLogMinerSplit,
                backFillLogMinerSplit.getStopOffset(),
                WatermarkKind.END);
    }

    public class SnapshotScnSplitChangeEventSourceContext
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
