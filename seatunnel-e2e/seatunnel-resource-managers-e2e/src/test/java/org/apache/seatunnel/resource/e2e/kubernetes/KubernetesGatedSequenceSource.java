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

package org.apache.seatunnel.resource.e2e.kubernetes;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.source.checkpointable.CheckpointableSequenceSource;
import org.apache.seatunnel.e2e.source.checkpointable.CheckpointableSequenceSourceFactory;
import org.apache.seatunnel.e2e.source.checkpointable.CheckpointableSequenceSplit;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/** Test-image-only bounded source: checkpoint a committed prefix while holding a known tail. */
public final class KubernetesGatedSequenceSource extends CheckpointableSequenceSource {
    static final String RELEASE = "/tmp/ha-tail-release";
    static final String REACHED = "/tmp/ha-tail-reached";
    static final int PREFIX = 120;
    static final int TOTAL = 240;

    public KubernetesGatedSequenceSource() {
        super(ReadonlyConfig.fromMap(java.util.Collections.emptyMap()));
    }

    @Override
    public String getPluginName() {
        return "KubernetesGatedSequence";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, CheckpointableSequenceSplit> createReader(
            SourceReader.Context context) {
        return new Reader(context);
    }

    /** Factory and classes are packaged explicitly into the disposable image, never shipped. */
    public static final class Factory extends CheckpointableSequenceSourceFactory {
        @Override
        public String factoryIdentifier() {
            return "KubernetesGatedSequence";
        }

        @Override
        public OptionRule optionRule() {
            return OptionRule.builder().build();
        }

        @Override
        public <T, SplitT extends SourceSplit, StateT extends Serializable>
                TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
            return () -> (SeaTunnelSource<T, SplitT, StateT>) new KubernetesGatedSequenceSource();
        }

        @Override
        public Class<? extends SeaTunnelSource> getSourceClass() {
            return KubernetesGatedSequenceSource.class;
        }
    }

    @Override
    public org.apache.seatunnel.api.source.SourceSplitEnumerator<
                    CheckpointableSequenceSplit,
                    org.apache.seatunnel.e2e.source.checkpointable.CheckpointableSequenceState>
            createEnumerator(
                    org.apache.seatunnel.api.source.SourceSplitEnumerator.Context<
                                    CheckpointableSequenceSplit>
                            context) {
        return new org.apache.seatunnel.e2e.source.checkpointable
                .CheckpointableSequenceSplitEnumerator(
                context,
                java.util.Collections.singletonList(
                        new CheckpointableSequenceSplit("sequence", 0, TOTAL, 0)),
                false);
    }

    public static final class Reader
            implements SourceReader<SeaTunnelRow, CheckpointableSequenceSplit> {
        private final Context context;
        private final Object stateLock = new Object();
        private final List<CheckpointableSequenceSplit> splits = new ArrayList<>();
        private boolean noMoreSplits;

        Reader(Context context) {
            this.context = context;
        }

        @Override
        public void open() {}

        @Override
        public void close() {}

        @Override
        public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
            synchronized (output.getCheckpointLock()) {
                synchronized (stateLock) {
                    for (CheckpointableSequenceSplit split : splits) {
                        if (split.hasRemaining()) {
                            if (split.getNextOffset() >= PREFIX
                                    && !Files.exists(Paths.get(RELEASE))) {
                                if (!Files.exists(Paths.get(REACHED))) {
                                    Files.writeString(
                                            Paths.get(REACHED),
                                            Long.toString(split.getNextOffset()));
                                }
                            } else {
                                output.collect(new SeaTunnelRow(new Object[] {split.advance()}));
                            }
                            break;
                        }
                    }
                    if (noMoreSplits
                            && splits.stream()
                                    .noneMatch(CheckpointableSequenceSplit::hasRemaining)) {
                        context.signalNoMoreElement();
                    }
                }
            }
            Thread.sleep(10);
        }

        @Override
        public List<CheckpointableSequenceSplit> snapshotState(long checkpointId) {
            synchronized (stateLock) {
                List<CheckpointableSequenceSplit> snapshot = new ArrayList<>();
                for (CheckpointableSequenceSplit split : splits) {
                    snapshot.add(
                            new CheckpointableSequenceSplit(
                                    split.splitId(),
                                    split.getStartOffset(),
                                    split.getEndOffset(),
                                    split.getNextOffset()));
                }
                return snapshot;
            }
        }

        @Override
        public void addSplits(List<CheckpointableSequenceSplit> restored) {
            synchronized (stateLock) {
                splits.addAll(restored);
            }
        }

        @Override
        public void handleNoMoreSplits() {
            synchronized (stateLock) {
                noMoreSplits = true;
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}
    }
}
