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

package org.apache.seatunnel.translation.source;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.event.EnumeratorCloseEvent;
import org.apache.seatunnel.api.source.event.EnumeratorOpenEvent;
import org.apache.seatunnel.api.source.event.ReaderCloseEvent;
import org.apache.seatunnel.api.source.event.ReaderOpenEvent;
import org.apache.seatunnel.translation.util.ThreadPoolExecutorFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class CoordinatedSource<T, SplitT extends SourceSplit, StateT extends Serializable>
        implements BaseSourceFunction<T> {
    protected static final long SLEEP_TIME_INTERVAL = 5L;
    protected final SeaTunnelSource<T, SplitT, StateT> source;
    protected final Map<Integer, List<byte[]>> restoredState;
    protected final Integer parallelism;
    protected final String jobId;

    protected final Serializer<SplitT> splitSerializer;
    protected final Serializer<StateT> enumeratorStateSerializer;

    protected final CoordinatedEnumeratorContext<SplitT> coordinatedEnumeratorContext;
    protected final Map<Integer, CoordinatedReaderContext> readerContextMap;
    protected final Map<Integer, List<SplitT>> restoredSplitStateMap = new HashMap<>();

    protected transient volatile SourceSplitEnumerator<SplitT, StateT> splitEnumerator;
    protected transient Map<Integer, SourceReader<T, SplitT>> readerMap = new ConcurrentHashMap<>();
    protected final Map<Integer, AtomicBoolean> readerRunningMap;
    protected final AtomicInteger completedReader = new AtomicInteger(0);
    protected transient volatile ScheduledThreadPoolExecutor executorService;

    /** Flag indicating whether the consumer is still running. */
    protected volatile boolean running = true;

    public CoordinatedSource(
            SeaTunnelSource<T, SplitT, StateT> source,
            Map<Integer, List<byte[]>> restoredState,
            int parallelism,
            String jobId) {
        this.source = source;
        this.restoredState = restoredState;
        this.parallelism = parallelism;
        this.jobId = jobId;
        this.splitSerializer = source.getSplitSerializer();
        this.enumeratorStateSerializer = source.getEnumeratorStateSerializer();

        this.coordinatedEnumeratorContext = new CoordinatedEnumeratorContext<>(this, jobId);
        this.readerContextMap = new ConcurrentHashMap<>(parallelism);
        this.readerRunningMap = new ConcurrentHashMap<>(parallelism);
        try {
            createSplitEnumerator();
            createReaders();
        } catch (Exception e) {
            log.warn("create split enumerator or readers failed", e);
        }
    }

    private void createSplitEnumerator() throws Exception {
        if (restoredState != null && restoredState.size() > 0) {
            StateT restoredEnumeratorState = null;
            if (restoredState.containsKey(-1)) {
                restoredEnumeratorState =
                        enumeratorStateSerializer.deserialize(restoredState.get(-1).get(0));
            }
            splitEnumerator =
                    source.restoreEnumerator(coordinatedEnumeratorContext, restoredEnumeratorState);
            restoredState.forEach(
                    (subtaskId, splitBytes) -> {
                        if (subtaskId == -1) {
                            return;
                        }
                        List<SplitT> restoredSplitState = new ArrayList<>(splitBytes.size());
                        for (byte[] splitByte : splitBytes) {
                            try {
                                restoredSplitState.add(splitSerializer.deserialize(splitByte));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        restoredSplitStateMap.put(subtaskId, restoredSplitState);
                    });
        } else {
            splitEnumerator = source.createEnumerator(coordinatedEnumeratorContext);
        }
    }

    private void createReaders() throws Exception {
        for (int subtaskId = 0; subtaskId < this.parallelism; subtaskId++) {
            CoordinatedReaderContext readerContext =
                    new CoordinatedReaderContext(this, source.getBoundedness(), jobId, subtaskId);
            readerContextMap.put(subtaskId, readerContext);
            readerRunningMap.put(subtaskId, new AtomicBoolean(true));
            SourceReader<T, SplitT> reader = source.createReader(readerContext);
            readerMap.put(subtaskId, reader);
        }
    }

    @Override
    public void open() throws Exception {
        executorService =
                ThreadPoolExecutorFactory.createScheduledThreadPoolExecutor(
                        parallelism, "parallel-split-enumerator-executor");
        splitEnumerator.open();
        coordinatedEnumeratorContext.getEventListener().onEvent(new EnumeratorOpenEvent());
        restoredSplitStateMap.forEach(
                (subtaskId, splits) -> {
                    splitEnumerator.addSplitsBack(splits, subtaskId);
                });
        readerMap
                .entrySet()
                .parallelStream()
                .forEach(
                        entry -> {
                            try {
                                entry.getValue().open();
                                readerContextMap
                                        .get(entry.getKey())
                                        .getEventListener()
                                        .onEvent(new ReaderOpenEvent());
                                splitEnumerator.registerReader(entry.getKey());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    @Override
    public void run(Collector<T> collector) throws Exception {
        readerMap
                .entrySet()
                .parallelStream()
                .forEach(
                        entry -> {
                            final AtomicBoolean flag = readerRunningMap.get(entry.getKey());
                            final SourceReader<T, SplitT> reader = entry.getValue();
                            executorService.execute(
                                    () -> {
                                        while (flag.get()) {
                                            try {
                                                reader.pollNext(collector);
                                                if (collector.isEmptyThisPollNext()) {
                                                    Thread.sleep(100);
                                                } else {
                                                    collector.resetEmptyThisPollNext();
                                                    /**
                                                     * sleep(0) is used to prevent the current
                                                     * thread from occupying CPU resources for a
                                                     * long time, thus blocking the checkpoint
                                                     * thread for a long time. It is mentioned in
                                                     * this
                                                     * https://github.com/apache/seatunnel/issues/5694
                                                     */
                                                    Thread.sleep(0L);
                                                }
                                            } catch (Exception e) {
                                                running = false;
                                                flag.set(false);
                                                throw new RuntimeException(e);
                                            }
                                        }
                                    });
                        });
        splitEnumerator.run();
        while (running) {
            Thread.sleep(SLEEP_TIME_INTERVAL);
        }
    }

    @Override
    public void close() throws IOException {
        running = false;

        for (Map.Entry<Integer, SourceReader<T, SplitT>> entry : readerMap.entrySet()) {
            readerRunningMap.get(entry.getKey()).set(false);
            entry.getValue().close();
            readerContextMap.get(entry.getKey()).getEventListener().onEvent(new ReaderCloseEvent());
        }

        if (executorService != null) {
            executorService.shutdown();
        }

        try (SourceSplitEnumerator<SplitT, StateT> closed = splitEnumerator) {
            // just close the resources
            coordinatedEnumeratorContext.getEventListener().onEvent(new EnumeratorCloseEvent());
        }
    }

    // --------------------------------------------------------------------------------------------
    // Checkpoint & state
    // --------------------------------------------------------------------------------------------

    @Override
    public Map<Integer, List<byte[]>> snapshotState(long checkpointId) throws Exception {
        Map<Integer, List<byte[]>> allStates =
                readerMap
                        .entrySet()
                        .parallelStream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry<Integer, SourceReader<T, SplitT>>::getKey,
                                        readerEntry -> {
                                            try {
                                                List<SplitT> splitStates =
                                                        readerEntry
                                                                .getValue()
                                                                .snapshotState(checkpointId);
                                                final List<byte[]> rawValues =
                                                        new ArrayList<>(splitStates.size());
                                                for (SplitT splitState : splitStates) {
                                                    rawValues.add(
                                                            splitSerializer.serialize(splitState));
                                                }
                                                return rawValues;
                                            } catch (Exception e) {
                                                throw new RuntimeException(e);
                                            }
                                        }));
        StateT enumeratorState = splitEnumerator.snapshotState(checkpointId);
        if (enumeratorState != null) {
            byte[] enumeratorStateBytes = enumeratorStateSerializer.serialize(enumeratorState);
            allStates.put(-1, Collections.singletonList(enumeratorStateBytes));
        }
        return allStates;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        splitEnumerator.notifyCheckpointComplete(checkpointId);
        readerMap
                .values()
                .parallelStream()
                .forEach(
                        reader -> {
                            try {
                                reader.notifyCheckpointComplete(checkpointId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        splitEnumerator.notifyCheckpointAborted(checkpointId);
        readerMap
                .values()
                .parallelStream()
                .forEach(
                        reader -> {
                            try {
                                reader.notifyCheckpointAborted(checkpointId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    // --------------------------------------------------------------------------------------------
    // Reader context methods
    // --------------------------------------------------------------------------------------------

    protected void handleNoMoreElement(int subtaskId) {
        readerRunningMap.get(subtaskId).set(false);
        readerContextMap.remove(subtaskId);
        if (completedReader.incrementAndGet() == this.parallelism) {
            this.running = false;
        }
    }

    protected void handleSplitRequest(int subtaskId) {
        splitEnumerator.handleSplitRequest(subtaskId);
    }

    protected void handleReaderEvent(int subtaskId, SourceEvent event) {
        splitEnumerator.handleSourceEvent(subtaskId, event);
    }

    // --------------------------------------------------------------------------------------------
    // Enumerator context methods
    // --------------------------------------------------------------------------------------------

    public int currentReaderCount() {
        return readerContextMap.size();
    }

    public Set<Integer> registeredReaders() {
        return readerMap.keySet();
    }

    protected void addSplits(int subtaskId, List<SplitT> splits) {
        readerMap.get(subtaskId).addSplits(splits);
    }

    protected void handleNoMoreSplits(int subtaskId) {
        readerMap.get(subtaskId).handleNoMoreSplits();
    }

    protected void handleEnumeratorEvent(int subtaskId, SourceEvent event) {
        readerMap.get(subtaskId).handleSourceEvent(event);
    }
}
