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
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.state.CheckpointListener;
import org.apache.seatunnel.translation.util.ThreadPoolExecutorFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ParallelSource<T, SplitT extends SourceSplit, StateT> implements AutoCloseable, CheckpointListener {

    private final long splitEnumeratorTimeInterval = 5L;

    protected final SeaTunnelSource<T, SplitT, StateT> source;
    protected final ParallelEnumeratorContext<SplitT> parallelEnumeratorContext;
    protected final ParallelReaderContext readerContext;
    protected final Integer subtaskId;
    protected final Integer parallelism;

    protected final Serializer<SplitT> splitSerializer;
    protected final Serializer<StateT> enumeratorStateSerializer;

    protected final List<SplitT> restoredSplitState;

    protected transient volatile SourceSplitEnumerator<SplitT, StateT> splitEnumerator;
    protected transient volatile SourceReader<T, SplitT> reader;
    protected transient volatile ScheduledThreadPoolExecutor executorService;

    /**
     * Flag indicating whether the consumer is still running.
     */
    private volatile boolean running = true;

    public ParallelSource(SeaTunnelSource<T, SplitT, StateT> source,
                          List<byte[]> restoredState,
                          int parallelism,
                          int subtaskId) {
        this.source = source;
        this.subtaskId = subtaskId;
        this.parallelism = parallelism;

        this.splitSerializer = source.getSplitSerializer();
        this.enumeratorStateSerializer = source.getEnumeratorStateSerializer();
        this.parallelEnumeratorContext = new ParallelEnumeratorContext<>(this, parallelism, subtaskId);
        this.readerContext = new ParallelReaderContext(this, source.getBoundedness(), subtaskId);

        // Create or restore split enumerator & reader
        try {
            if (restoredState != null && restoredState.size() > 0) {
                StateT restoredEnumeratorState = enumeratorStateSerializer.deserialize(restoredState.get(0));
                restoredSplitState = new ArrayList<>(restoredState.size());
                for (int i = 1; i < restoredState.size(); i++) {
                    restoredSplitState.add(splitSerializer.deserialize(restoredState.get(i)));
                }

                splitEnumerator = source.restoreEnumerator(parallelEnumeratorContext, restoredEnumeratorState);
            } else {
                restoredSplitState = Collections.emptyList();
                splitEnumerator = source.createEnumerator(parallelEnumeratorContext);
            }
            reader = source.createReader(readerContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private transient volatile Thread splitEnumeratorThread;

    public void open() throws Exception {
        executorService = ThreadPoolExecutorFactory.createScheduledThreadPoolExecutor(1, String.format("parallel-split-enumerator-executor-%s", subtaskId));
        splitEnumerator.open();
        splitEnumerator.addSplitsBack(restoredSplitState, subtaskId);
        reader.open();
        parallelEnumeratorContext.register();
        splitEnumerator.registerReader(subtaskId);
    }

    public void run(Collector<T> collector) throws Exception {
        Future<?> future = executorService.submit(() -> {
            try {
                splitEnumerator.run();
            } catch (Exception e) {
                // TODO if split enumerator failed, the main thread will not shutdown.
                throw new RuntimeException("SourceSplitEnumerator run failed.", e);
            }
        });

        while (running) {
            if (future.isDone()) {
                future.get();
            }
            reader.pollNext(collector);
        }
    }

    @Override
    public void close() throws IOException {
        // set ourselves as not running;
        // this would let the main discovery loop escape as soon as possible
        running = false;

        if (executorService != null) {
            executorService.shutdown();
        }

        if (splitEnumerator != null) {
            splitEnumerator.close();
        }

        if (reader != null) {
            reader.close();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Reader context methods
    // --------------------------------------------------------------------------------------------

    protected void handleNoMoreElement() {
        running = false;
    }

    protected void handleSplitRequest(int subtaskId) {
        splitEnumerator.handleSplitRequest(subtaskId);
    }

    // --------------------------------------------------------------------------------------------
    // Enumerator context methods
    // --------------------------------------------------------------------------------------------

    protected void addSplits(List<SplitT> splits) {
        reader.addSplits(splits);
    }

    protected void handleNoMoreSplits() {
        reader.handleNoMoreSplits();
    }

    public List<byte[]> snapshotState(long checkpointId) throws Exception {
        StateT enumeratorState = splitEnumerator.snapshotState(checkpointId);
        byte[] enumeratorStateBytes = enumeratorStateSerializer.serialize(enumeratorState);
        List<SplitT> splitStates = reader.snapshotState(checkpointId);
        final List<byte[]> rawValues = new ArrayList<>(splitStates.size() + 1);
        rawValues.add(enumeratorStateBytes);
        for (SplitT splitState : splitStates) {
            rawValues.add(splitSerializer.serialize(splitState));
        }
        return rawValues;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        splitEnumerator.notifyCheckpointComplete(checkpointId);
        reader.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        splitEnumerator.notifyCheckpointAborted(checkpointId);
        reader.notifyCheckpointAborted(checkpointId);
    }
}
