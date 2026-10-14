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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsReportEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.translation.source.CoordinatedSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Runs the real enumerator and hybrid split assigner through the coordinated source that the Spark
 * engine uses, where all readers share one enumerator and no engine checkpoint is ever taken.
 */
class IncrementalSourceEnumeratorTest {

    private static final TableId TABLE_ID = TableId.parse("db1.table1");

    @Test
    void shouldEnterIncrementalPhaseWithMultipleReadersAndEnumeratorCheckpoints() throws Exception {
        int parallelism = 2;
        List<String> snapshotSplitIds =
                IntStream.range(0, 4)
                        .mapToObj(i -> TABLE_ID + ":" + i)
                        .collect(Collectors.toList());
        ReportingSource source = new ReportingSource(snapshotSplitIds);
        CoordinatedSource<String, SourceSplitBase, PendingSplitsState> coordinatedSource =
                new CoordinatedSource<String, SourceSplitBase, PendingSplitsState>(
                        source, null, parallelism, "incremental-source-enumerator") {
                    @Override
                    protected long getEnumeratorCheckpointInterval() {
                        return 10L;
                    }
                };

        Assertions.assertEquals(parallelism, source.enumeratorParallelism);

        RecordCollector collector = new RecordCollector();
        coordinatedSource.open();
        Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(30), () -> coordinatedSource.run(collector));
        coordinatedSource.close();

        List<String> expected = new ArrayList<>(snapshotSplitIds);
        expected.add("incremental-split-0");
        Assertions.assertEquals(
                expected, collector.records.stream().sorted().collect(Collectors.toList()));
    }

    private static final class RecordCollector implements Collector<String> {

        private final Object lock = new Object();
        private final Queue<String> records = new ConcurrentLinkedQueue<>();

        @Override
        public void collect(String record) {
            records.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }

    /** Builds the real enumerator over a fixed set of snapshot splits of one table. */
    private static final class ReportingSource
            implements SeaTunnelSource<String, SourceSplitBase, PendingSplitsState>,
                    SupportCoordinate {

        private final List<String> snapshotSplitIds;
        private int enumeratorParallelism = -1;

        private ReportingSource(List<String> snapshotSplitIds) {
            this.snapshotSplitIds = snapshotSplitIds;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SourceReader<String, SourceSplitBase> createReader(
                SourceReader.Context readerContext) {
            return new ReportingReader(readerContext);
        }

        @Override
        public SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> createEnumerator(
                SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext) {
            enumeratorParallelism = enumeratorContext.currentParallelism();
            List<SnapshotSplit> remainingSplits =
                    snapshotSplitIds.stream()
                            .map(id -> new SnapshotSplit(id, TABLE_ID, null, null, null))
                            .collect(Collectors.toList());
            SnapshotPhaseState snapshotPhaseState =
                    new SnapshotPhaseState(
                            Collections.singletonList(TABLE_ID),
                            remainingSplits,
                            new HashMap<>(),
                            new HashMap<>(),
                            false,
                            Collections.emptyList(),
                            false,
                            true);
            SplitAssigner.Context<SourceConfig> assignerContext =
                    new SplitAssigner.Context<>(
                            new TestSourceConfig(),
                            Collections.singleton(TABLE_ID),
                            new HashMap<>(),
                            new HashMap<>());
            HybridSplitAssigner<SourceConfig> splitAssigner =
                    new HybridSplitAssigner<>(
                            assignerContext,
                            enumeratorParallelism,
                            1,
                            new HybridPendingSplitsState(snapshotPhaseState, null),
                            testDialect(),
                            new TestOffsetFactory());
            return new IncrementalSourceEnumerator(enumeratorContext, splitAssigner);
        }

        @Override
        public SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> restoreEnumerator(
                SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext,
                PendingSplitsState checkpointState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getPluginName() {
            return "reporting-cdc-source";
        }
    }

    /**
     * Follows the protocol of the CDC reader: it reports every finished snapshot split to the
     * enumerator and then asks for the next split.
     */
    private static final class ReportingReader implements SourceReader<String, SourceSplitBase> {

        private final SourceReader.Context context;
        private final Queue<SourceSplitBase> splits = new ConcurrentLinkedQueue<>();
        private volatile boolean started;
        private volatile boolean noMoreSplits;

        private ReportingReader(SourceReader.Context context) {
            this.context = context;
        }

        @Override
        public void open() {}

        @Override
        public void close() {}

        @Override
        public void pollNext(Collector<String> output) {
            if (!started) {
                started = true;
                context.sendSplitRequest();
                return;
            }
            SourceSplitBase split = splits.poll();
            if (split == null) {
                if (noMoreSplits) {
                    context.signalNoMoreElement();
                }
                return;
            }
            output.collect(split.splitId());
            if (split.isSnapshotSplit()) {
                CompletedSnapshotSplitsReportEvent reportEvent =
                        new CompletedSnapshotSplitsReportEvent();
                reportEvent.setCompletedSnapshotSplitWatermarks(
                        Collections.singletonList(
                                new SnapshotSplitWatermark(
                                        split.splitId(), new TestOffset(1L), new TestOffset(2L))));
                context.sendSourceEventToEnumerator(reportEvent);
            } else {
                IncrementalSplit incrementalSplit = split.asIncrementalSplit();
                Assertions.assertEquals(
                        Collections.singletonList(TABLE_ID), incrementalSplit.getTableIds());
            }
            context.sendSplitRequest();
        }

        @Override
        public List<SourceSplitBase> snapshotState(long checkpointId) {
            return new ArrayList<>(splits);
        }

        @Override
        public void addSplits(List<SourceSplitBase> splits) {
            this.splits.addAll(splits);
        }

        @Override
        public void handleNoMoreSplits() {
            noMoreSplits = true;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}
    }

    @SuppressWarnings("unchecked")
    private static DataSourceDialect<SourceConfig> testDialect() {
        ChunkSplitter chunkSplitter =
                tableId -> {
                    throw new UnsupportedOperationException();
                };
        return (DataSourceDialect<SourceConfig>)
                Proxy.newProxyInstance(
                        IncrementalSourceEnumeratorTest.class.getClassLoader(),
                        new Class<?>[] {DataSourceDialect.class},
                        (proxy, method, args) -> {
                            if ("createChunkSplitter".equals(method.getName())) {
                                return chunkSplitter;
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }

    private static final class TestSourceConfig implements SourceConfig {

        private static final long serialVersionUID = 1L;

        @Override
        public StartupConfig getStartupConfig() {
            return null;
        }

        @Override
        public StopConfig getStopConfig() {
            return new StopConfig(StopMode.NEVER, null, null, null);
        }

        @Override
        public int getSplitSize() {
            return 1;
        }

        @Override
        public boolean isExactlyOnce() {
            return false;
        }
    }

    private static final class TestOffsetFactory extends OffsetFactory {

        private static final long serialVersionUID = 1L;

        @Override
        public Offset earliest() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Offset neverStop() {
            return new TestOffset(Long.MAX_VALUE);
        }

        @Override
        public Offset latest() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Offset specific(Map<String, String> offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Offset specific(String filename, Long position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Offset timestamp(long timestamp) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class TestOffset extends Offset {

        private static final long serialVersionUID = 1L;

        private TestOffset(long value) {
            this.offset = Collections.singletonMap("pos", String.valueOf(value));
        }

        @Override
        public int compareTo(Offset other) {
            return Long.compare(
                    Long.parseLong(this.offset.get("pos")),
                    Long.parseLong(other.getOffset().get("pos")));
        }
    }
}
