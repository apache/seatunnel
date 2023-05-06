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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.snapshot;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.PostgresSourceFetchTaskContext;

import io.debezium.pipeline.spi.SnapshotResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

public class PostgresSnapshotFetchTask implements FetchTask<SourceSplitBase> {

    private final SnapshotSplit split;

    private volatile boolean taskRunning = false;

    private PostgresSnapshotSplitReadTask snapshotSplitReadTask;

    public PostgresSnapshotFetchTask(SnapshotSplit split) {
        this.split = split;
    }

    @Override
    public void execute(FetchTask.Context context) throws Exception {
        PostgresSourceFetchTaskContext sourceFetchContext =
                (PostgresSourceFetchTaskContext) context;
        taskRunning = true;
        snapshotSplitReadTask =
                new PostgresSnapshotSplitReadTask(
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

        final IncrementalSplit backfillBinlogSplit =
                createBackFillWalSplit(changeEventSourceContext);

        // optimization that skip the binlog read when the low watermark equals high
        // watermark
        final boolean binlogBackfillRequired =
                backfillBinlogSplit.getStopOffset().isAfter(backfillBinlogSplit.getStartupOffset());
        if (true) {
            dispatchBinlogEndEvent(
                    backfillBinlogSplit,
                    ((PostgresSourceFetchTaskContext) context).getOffsetContext().getPartition(),
                    ((PostgresSourceFetchTaskContext) context).getDispatcher());
            taskRunning = false;
            return;
        }
    }

    private IncrementalSplit createBackFillWalSplit(
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
}
