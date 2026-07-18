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

package org.apache.seatunnel.connectors.seatunnel.sink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkAggregatedCommitter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SinkFlowTestUtils {

    public static void runBatchWithCheckpointDisabled(
            CatalogTable catalogTable,
            ReadonlyConfig options,
            TableSinkFactory<SeaTunnelRow, ?, ?, ?> factory,
            List<SeaTunnelRow> rows)
            throws IOException {
        JobContext context = new JobContext(System.currentTimeMillis());
        context.setJobMode(JobMode.BATCH);
        context.setEnableCheckpoint(false);
        runWithContext(catalogTable, options, factory, rows, context, 1);
    }

    public static void runBatchWithCheckpointEnabled(
            CatalogTable catalogTable,
            ReadonlyConfig options,
            TableSinkFactory<SeaTunnelRow, ?, ?, ?> factory,
            List<SeaTunnelRow> rows)
            throws IOException {
        runBatchWithCheckpointEnabled(
                catalogTable,
                options,
                factory,
                rows,
                PeriodicCheckpointOptions.defaultSingleCheckpoint());
    }

    public static void runBatchWithCheckpointEnabled(
            CatalogTable catalogTable,
            ReadonlyConfig options,
            TableSinkFactory<SeaTunnelRow, ?, ?, ?> factory,
            List<SeaTunnelRow> rows,
            PeriodicCheckpointOptions checkpointOptions)
            throws IOException {
        JobContext context = new JobContext(System.currentTimeMillis());
        context.setJobMode(JobMode.BATCH);
        context.setEnableCheckpoint(true);
        runWithContext(catalogTable, options, factory, rows, context, 1, checkpointOptions);
    }

    public static void runParallelSubtasksBatchWithCheckpointDisabled(
            CatalogTable catalogTable,
            ReadonlyConfig options,
            TableSinkFactory<SeaTunnelRow, ?, ?, ?> factory,
            List<SeaTunnelRow> rows,
            int parallelism)
            throws IOException {
        JobContext context = new JobContext(System.currentTimeMillis());
        context.setJobMode(JobMode.BATCH);
        context.setEnableCheckpoint(false);
        runWithContext(catalogTable, options, factory, rows, context, parallelism);
    }

    public static void runBatchWithMultiTableSink(
            TableSinkFactory<SeaTunnelRow, ?, ?, ?> factory,
            TableSinkFactoryContext tableSinkFactoryContext,
            List<SeaTunnelRow> rows,
            boolean checkpointEnabled,
            int parallelism)
            throws IOException {
        runBatchWithMultiTableSink(
                factory,
                tableSinkFactoryContext,
                rows,
                checkpointEnabled,
                parallelism,
                checkpointEnabled
                        ? PeriodicCheckpointOptions.defaultSingleCheckpoint()
                        : PeriodicCheckpointOptions.neverTrigger());
    }

    public static void runBatchWithMultiTableSink(
            TableSinkFactory<SeaTunnelRow, ?, ?, ?> factory,
            TableSinkFactoryContext tableSinkFactoryContext,
            List<SeaTunnelRow> rows,
            boolean checkpointEnabled,
            int parallelism,
            PeriodicCheckpointOptions checkpointOptions)
            throws IOException {
        JobContext context = new JobContext(System.currentTimeMillis());
        context.setJobMode(JobMode.BATCH);
        context.setEnableCheckpoint(checkpointEnabled);
        runWithContext(
                factory,
                tableSinkFactoryContext,
                rows,
                context,
                parallelism,
                checkpointEnabled ? checkpointOptions : PeriodicCheckpointOptions.neverTrigger());
    }

    private static void runWithContext(
            CatalogTable catalogTable,
            ReadonlyConfig options,
            TableSinkFactory<SeaTunnelRow, ?, ?, ?> factory,
            List<SeaTunnelRow> rows,
            JobContext context,
            int parallelism)
            throws IOException {

        TableSinkFactoryContext tableSinkFactoryContext =
                new TableSinkFactoryContext(
                        catalogTable, options, Thread.currentThread().getContextClassLoader());

        runWithContext(
                factory,
                tableSinkFactoryContext,
                rows,
                context,
                parallelism,
                context.isEnableCheckpoint()
                        ? PeriodicCheckpointOptions.defaultSingleCheckpoint()
                        : PeriodicCheckpointOptions.neverTrigger());
    }

    private static void runWithContext(
            CatalogTable catalogTable,
            ReadonlyConfig options,
            TableSinkFactory<SeaTunnelRow, ?, ?, ?> factory,
            List<SeaTunnelRow> rows,
            JobContext context,
            int parallelism,
            PeriodicCheckpointOptions checkpointOptions)
            throws IOException {

        TableSinkFactoryContext tableSinkFactoryContext =
                new TableSinkFactoryContext(
                        catalogTable, options, Thread.currentThread().getContextClassLoader());

        runWithContext(
                factory, tableSinkFactoryContext, rows, context, parallelism, checkpointOptions);
    }

    private static void runWithContext(
            TableSinkFactory<SeaTunnelRow, ?, ?, ?> factory,
            TableSinkFactoryContext tableSinkFactoryContext,
            List<SeaTunnelRow> rows,
            JobContext context,
            int parallelism,
            PeriodicCheckpointOptions checkpointOptions)
            throws IOException {
        SeaTunnelSink<SeaTunnelRow, ?, ?, ?> sink =
                factory.createSink(tableSinkFactoryContext).createSink();
        sink.setJobContext(context);
        List<List<Object>> writerCheckpointInfos =
                IntStream.range(0, parallelism)
                        .mapToObj(i -> Collections.synchronizedList(new ArrayList<>()))
                        .collect(Collectors.toList());

        List<Throwable> asyncErrors = Collections.synchronizedList(new ArrayList<>());
        IntStream.range(0, parallelism)
                .parallel()
                .forEach(
                        writerIndex -> {
                            try {
                                runWriter(
                                        sink,
                                        rows,
                                        checkpointOptions,
                                        writerIndex,
                                        parallelism,
                                        writerCheckpointInfos.get(writerIndex));
                            } catch (Throwable t) {
                                t.addSuppressed(
                                        new RuntimeException("Writer " + writerIndex + " failed"));
                                asyncErrors.add(t);
                            }
                        });

        if (!asyncErrors.isEmpty()) {
            rethrow(asyncErrors.get(0));
        }

        LinkedHashMap<Long, List<Object>> checkpointCommitInfos =
                buildCheckpointMap(writerCheckpointInfos);

        Optional<? extends SinkCommitter<?>> sinkCommitter = sink.createCommitter();
        Optional<? extends SinkAggregatedCommitter<?, ?>> aggregatedCommitterOptional =
                sink.createAggregatedCommitter();

        if (!checkpointCommitInfos.isEmpty()) {
            if (aggregatedCommitterOptional.isPresent()) {
                SinkAggregatedCommitter<?, ?> aggregatedCommitter =
                        aggregatedCommitterOptional.get();
                MultiTableResourceManager resourceManager = null;
                if (aggregatedCommitter instanceof SupportMultiTableSinkAggregatedCommitter) {
                    resourceManager =
                            ((SupportMultiTableSinkAggregatedCommitter<?>) aggregatedCommitter)
                                    .initMultiTableResourceManager(1, 1);
                }
                aggregatedCommitter.init();
                if (resourceManager != null) {
                    ((SupportMultiTableSinkAggregatedCommitter<?>) aggregatedCommitter)
                            .setMultiTableResourceManager(resourceManager, 0);
                }

                for (List<Object> commitInfos : checkpointCommitInfos.values()) {
                    Object aggregatedCommitInfoT =
                            ((SinkAggregatedCommitter) aggregatedCommitter).combine(commitInfos);
                    ((SinkAggregatedCommitter) aggregatedCommitter)
                            .commit(Collections.singletonList(aggregatedCommitInfoT));
                }
                aggregatedCommitter.close();
            } else if (sinkCommitter.isPresent()) {
                SinkCommitter sinkCommitterInstance = (SinkCommitter) sinkCommitter.get();
                for (List<Object> commitInfos : checkpointCommitInfos.values()) {
                    sinkCommitterInstance.commit(commitInfos);
                }
            } else {
                throw new RuntimeException("No committer found");
            }
        }
    }

    private static void runWriter(
            SeaTunnelSink<SeaTunnelRow, ?, ?, ?> sink,
            List<SeaTunnelRow> rows,
            PeriodicCheckpointOptions checkpointOptions,
            int writerIndex,
            int parallelism,
            List<Object> currentWriterCommits)
            throws IOException {
        SinkWriter<SeaTunnelRow, ?, ?> sinkWriter =
                sink.createWriter(new DefaultSinkWriterContext(writerIndex, parallelism));
        long lastCheckpointTs = System.currentTimeMillis();
        int recordsSinceLastCheckpoint = 0;
        CheckpointState checkpointState = new CheckpointState();
        for (SeaTunnelRow row : rows) {
            sinkWriter.write(row);
            recordsSinceLastCheckpoint++;
            if (shouldTriggerCheckpoint(
                            checkpointOptions, recordsSinceLastCheckpoint, lastCheckpointTs)
                    && triggerCheckpoint(
                            sinkWriter,
                            checkpointOptions,
                            checkpointState,
                            currentWriterCommits,
                            false)) {
                recordsSinceLastCheckpoint = 0;
                lastCheckpointTs = System.currentTimeMillis();
            }
        }
        boolean needsFinalCheckpoint =
                recordsSinceLastCheckpoint > 0
                        || checkpointState.triggeredCount == 0
                        || checkpointOptions.isTriggerOnFinish();
        if (needsFinalCheckpoint) {
            triggerCheckpoint(
                    sinkWriter, checkpointOptions, checkpointState, currentWriterCommits, true);
        }
        sinkWriter.close();
    }

    private static boolean shouldTriggerCheckpoint(
            PeriodicCheckpointOptions options,
            int recordsSinceLastCheckpoint,
            long lastCheckpointTs) {
        if (!options.enablePeriodicTrigger()) {
            return false;
        }
        boolean triggerByRecord =
                options.getRecordsPerCheckpoint() > 0
                        && recordsSinceLastCheckpoint >= options.getRecordsPerCheckpoint();
        boolean triggerByInterval =
                options.getIntervalMillis() > 0
                        && (System.currentTimeMillis() - lastCheckpointTs)
                                >= options.getIntervalMillis();
        return triggerByRecord || triggerByInterval;
    }

    private static boolean triggerCheckpoint(
            SinkWriter<SeaTunnelRow, ?, ?> sinkWriter,
            PeriodicCheckpointOptions options,
            CheckpointState checkpointState,
            List<Object> writerCheckpointInfos,
            boolean force)
            throws IOException {
        if (!force && !options.canTrigger(checkpointState.triggeredCount)) {
            return false;
        }
        long checkpointId = checkpointState.nextCheckpointId();
        Optional<?> commitInfo = sinkWriter.prepareCommit(checkpointId);
        sinkWriter.snapshotState(checkpointId);
        if (commitInfo.isPresent()) {
            writerCheckpointInfos.add(commitInfo.get());
        }
        checkpointState.incrementTriggeredCount();
        return true;
    }

    private static LinkedHashMap<Long, List<Object>> buildCheckpointMap(
            List<List<Object>> writerCheckpointInfos) {
        LinkedHashMap<Long, List<Object>> checkpointCommitInfos = new LinkedHashMap<>();
        int rounds = 0;
        for (List<Object> infos : writerCheckpointInfos) {
            rounds = Math.max(rounds, infos.size());
        }
        long checkpointId = 1L;
        for (int round = 0; round < rounds; round++) {
            List<Object> aggregatedInfos = new ArrayList<>();
            for (List<Object> writerInfos : writerCheckpointInfos) {
                if (round < writerInfos.size()) {
                    aggregatedInfos.add(writerInfos.get(round));
                }
            }
            if (!aggregatedInfos.isEmpty()) {
                checkpointCommitInfos.put(checkpointId++, aggregatedInfos);
            }
        }
        return checkpointCommitInfos;
    }

    private static class CheckpointState {
        private long checkpointId = 1L;
        private int triggeredCount = 0;

        private long nextCheckpointId() {
            return checkpointId++;
        }

        private void incrementTriggeredCount() {
            triggeredCount++;
        }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void rethrow(Throwable throwable) throws E {
        throw (E) throwable;
    }

    public static final class PeriodicCheckpointOptions {
        private final int recordsPerCheckpoint;
        private final long intervalMillis;
        private final int maxCheckpointCount;
        private final boolean triggerOnFinish;

        private PeriodicCheckpointOptions(Builder builder) {
            this.recordsPerCheckpoint = builder.recordsPerCheckpoint;
            this.intervalMillis = builder.intervalMillis;
            this.maxCheckpointCount = builder.maxCheckpointCount;
            this.triggerOnFinish = builder.triggerOnFinish;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static PeriodicCheckpointOptions defaultSingleCheckpoint() {
            return builder().maxCheckpointCount(1).triggerOnFinish(true).build();
        }

        public static PeriodicCheckpointOptions neverTrigger() {
            return builder().maxCheckpointCount(0).triggerOnFinish(false).build();
        }

        public int getRecordsPerCheckpoint() {
            return recordsPerCheckpoint;
        }

        public long getIntervalMillis() {
            return intervalMillis;
        }

        public boolean isTriggerOnFinish() {
            return triggerOnFinish;
        }

        private boolean enablePeriodicTrigger() {
            return recordsPerCheckpoint > 0 || intervalMillis > 0;
        }

        private boolean canTrigger(int triggeredCount) {
            return maxCheckpointCount <= 0 || triggeredCount < maxCheckpointCount;
        }

        public static final class Builder {
            private int recordsPerCheckpoint = 0;
            private long intervalMillis = 0L;
            private int maxCheckpointCount = 1;
            private boolean triggerOnFinish = true;

            public Builder recordsPerCheckpoint(int recordsPerCheckpoint) {
                if (recordsPerCheckpoint < 0) {
                    throw new IllegalArgumentException("recordsPerCheckpoint must be >= 0");
                }
                this.recordsPerCheckpoint = recordsPerCheckpoint;
                return this;
            }

            public Builder intervalMillis(long intervalMillis) {
                if (intervalMillis < 0) {
                    throw new IllegalArgumentException("intervalMillis must be >= 0");
                }
                this.intervalMillis = intervalMillis;
                return this;
            }

            public Builder maxCheckpointCount(int maxCheckpointCount) {
                this.maxCheckpointCount = maxCheckpointCount;
                return this;
            }

            public Builder triggerOnFinish(boolean triggerOnFinish) {
                this.triggerOnFinish = triggerOnFinish;
                return this;
            }

            public PeriodicCheckpointOptions build() {
                return new PeriodicCheckpointOptions(this);
            }
        }
    }
}
