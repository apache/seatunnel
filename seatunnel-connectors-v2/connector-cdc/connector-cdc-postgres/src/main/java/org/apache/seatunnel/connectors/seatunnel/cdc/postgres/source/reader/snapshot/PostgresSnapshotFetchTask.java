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
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.wal.PostgresWalFetchTask;

import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

@Slf4j
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
                        changeEventSourceContext,
                        sourceFetchContext.getPartition(),
                        sourceFetchContext.getOffsetContext());
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

        final IncrementalSplit backfillSplit = createBackFillWalSplit(changeEventSourceContext);
        // Skip WAL reading when no changes occurred or streaming is disabled for snapshot-only
        // startup.
        if (!changed || !sourceFetchContext.getSnapshotter().shouldStream()) {
            sourceFetchContext.closeReplicationConnection();
            dispatchBinlogEndEvent(
                    backfillSplit,
                    sourceFetchContext.getPartition().getSourcePartition(),
                    sourceFetchContext.getDispatcher());
            taskRunning = false;
            return;
        }

        PostgresWalFetchTask.PostgresWalSplitReadTask backfillReadTask =
                createBackfillWalSplitReadTask(backfillSplit, sourceFetchContext);
        log.info(
                "Start bounded WAL backfill for snapshot split {}, start offset: {}, stop offset: {}",
                split.splitId(),
                backfillSplit.getStartupOffset(),
                backfillSplit.getStopOffset());
        Throwable backfillFailure = null;
        try {
            backfillReadTask.execute(
                    new SnapshotWalSplitChangeEventSourceContext(),
                    sourceFetchContext.getPartition(),
                    sourceFetchContext.loadOffsetContext(backfillSplit.getStartupOffset()));
        } catch (Exception e) {
            backfillFailure = e;
            throw e;
        } catch (Error e) {
            backfillFailure = e;
            throw e;
        } finally {
            // Debezium closes the temporary connection. Explicitly drop and verify the slot
            // because Debezium otherwise logs and swallows shutdown failures.
            sourceFetchContext.releaseReplicationConnection();
            try {
                sourceFetchContext.dropBackfillReplicationSlot();
            } catch (RuntimeException cleanupFailure) {
                if (backfillFailure != null) {
                    backfillFailure.addSuppressed(cleanupFailure);
                } else {
                    throw cleanupFailure;
                }
            } finally {
                taskRunning = false;
            }
        }
        log.info("Bounded WAL backfill finished for snapshot split {}", split.splitId());
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

    /**
     * Creates the bounded Debezium WAL reader over the snapshot split's temporary replication slot.
     */
    private PostgresWalFetchTask.PostgresWalSplitReadTask createBackfillWalSplitReadTask(
            IncrementalSplit backfillSplit, PostgresSourceFetchTaskContext context) {
        return new PostgresWalFetchTask.PostgresWalSplitReadTask(
                context.getDbzConnectorConfig(),
                context.getSnapshotter(),
                context.getDataConnection(),
                context.getPgEventDispatcher(),
                context.getDispatcher(),
                context.getErrorHandler(),
                context.getDatabaseSchema(),
                context.getTaskContext(),
                context.getReplicationConnection(),
                backfillSplit);
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
     * Keeps the bounded WAL reader cancellable through the enclosing snapshot task lifecycle.
     *
     * <p>The Debezium loop observes {@link #taskRunning} while waiting for the high watermark.
     */
    private class SnapshotWalSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
