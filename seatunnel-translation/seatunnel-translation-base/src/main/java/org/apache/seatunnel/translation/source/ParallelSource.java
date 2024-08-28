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
import org.apache.seatunnel.api.source.event.EnumeratorCloseEvent;
import org.apache.seatunnel.api.source.event.EnumeratorOpenEvent;
import org.apache.seatunnel.api.source.event.ReaderCloseEvent;
import org.apache.seatunnel.api.source.event.ReaderOpenEvent;
import org.apache.seatunnel.translation.util.ThreadPoolExecutorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ParallelSource<T, SplitT extends SourceSplit, StateT extends Serializable>
        implements BaseSourceFunction<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelSource.class);

    protected final SeaTunnelSource<T, SplitT, StateT> source;
    protected final ParallelEnumeratorContext<SplitT> parallelEnumeratorContext;
    protected final ParallelReaderContext readerContext;
    protected final String jobId;
    protected final Integer subtaskId;
    protected final Integer parallelism;

    protected final Serializer<SplitT> splitSerializer;
    protected final Serializer<StateT> enumeratorStateSerializer;

    protected final List<SplitT> restoredSplitState;

    protected final SourceSplitEnumerator<SplitT, StateT> splitEnumerator;
    protected final SourceReader<T, SplitT> reader;
    protected transient volatile ScheduledThreadPoolExecutor executorService;

    /** Flag indicating whether the consumer is still running. */
    private volatile boolean running = true;

    public ParallelSource(
            SeaTunnelSource<T, SplitT, StateT> source,
            Map<Integer, List<byte[]>> restoredState,
            int parallelism,
            String jobId,
            int subtaskId) {
        this.source = source;
        this.jobId = jobId;
        this.subtaskId = subtaskId;
        this.parallelism = parallelism;

        this.splitSerializer = source.getSplitSerializer();
        this.enumeratorStateSerializer = source.getEnumeratorStateSerializer();
        this.parallelEnumeratorContext =
                new ParallelEnumeratorContext<>(this, parallelism, jobId, subtaskId);
        this.readerContext =
                new ParallelReaderContext(this, source.getBoundedness(), jobId, subtaskId);

        // Create or restore split enumerator & reader
        try {
            if (restoredState != null && restoredState.size() > 0) {
                StateT restoredEnumeratorState = null;
                if (restoredState.containsKey(-1)) {
                    restoredEnumeratorState =
                            enumeratorStateSerializer.deserialize(restoredState.get(-1).get(0));
                }
                restoredSplitState = new ArrayList<>(restoredState.get(subtaskId).size());
                for (byte[] splitBytes : restoredState.get(subtaskId)) {
                    restoredSplitState.add(splitSerializer.deserialize(splitBytes));
                }

                splitEnumerator =
                        source.restoreEnumerator(
                                parallelEnumeratorContext, restoredEnumeratorState);
            } else {
                restoredSplitState = Collections.emptyList();
                splitEnumerator = source.createEnumerator(parallelEnumeratorContext);
            }
            reader = source.createReader(readerContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void open() throws Exception {
        executorService =
                ThreadPoolExecutorFactory.createScheduledThreadPoolExecutor(
                        1, String.format("parallel-split-enumerator-executor-%s", subtaskId));
        splitEnumerator.open();
        if (restoredSplitState.size() > 0) {
            splitEnumerator.addSplitsBack(restoredSplitState, subtaskId);
        }
        reader.open();
        readerContext.getEventListener().onEvent(new ReaderOpenEvent());
        parallelEnumeratorContext.register();
        parallelEnumeratorContext.getEventListener().onEvent(new EnumeratorOpenEvent());
        splitEnumerator.registerReader(subtaskId);
    }

    @Override
    public void run(Collector<T> collector) throws Exception {
        Future<?> future =
                executorService.submit(
                        () -> {
                            try {
                                splitEnumerator.run();
                            } catch (Exception e) {
                                throw new RuntimeException("SourceSplitEnumerator run failed.", e);
                            }
                        });

        while (running) {
            if (future.isDone()) {
                future.get();
            }
            reader.pollNext(collector);
            if (collector.isEmptyThisPollNext()) {
                Thread.sleep(100);
            } else {
                collector.resetEmptyThisPollNext();
                /**
                 * sleep(0) is used to prevent the current thread from occupying CPU resources for a
                 * long time, thus blocking the checkpoint thread for a long time. It is mentioned
                 * in this https://github.com/apache/seatunnel/issues/5694
                 */
                Thread.sleep(0L);
            }
        }
        LOG.debug("Parallel source runs complete.");
    }

    @Override
    public void close() throws IOException {
        // set ourselves as not running;
        // this would let the main discovery loop escape as soon as possible
        running = false;

        if (executorService != null) {
            LOG.debug("Close the thread pool resource.");
            executorService.shutdown();
        }

        if (splitEnumerator != null) {
            LOG.debug("Close the split enumerator for the Apache SeaTunnel source.");
            splitEnumerator.close();
        }

        if (reader != null) {
            LOG.debug("Close the data reader for the Apache SeaTunnel source.");
            reader.close();
            readerContext.getEventListener().onEvent(new ReaderCloseEvent());
            parallelEnumeratorContext.getEventListener().onEvent(new EnumeratorCloseEvent());
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

    // --------------------------------------------------------------------------------------------
    // Checkpoint & state
    // --------------------------------------------------------------------------------------------

    @Override
    public Map<Integer, List<byte[]>> snapshotState(long checkpointId) throws Exception {
        Map<Integer, List<byte[]>> allStates = new HashMap<>(2);

        StateT enumeratorState = splitEnumerator.snapshotState(checkpointId);
        if (enumeratorState != null) {
            byte[] enumeratorStateBytes = enumeratorStateSerializer.serialize(enumeratorState);
            allStates.put(-1, Collections.singletonList(enumeratorStateBytes));
        }
        List<SplitT> splitStates = reader.snapshotState(checkpointId);
        if (splitStates != null) {
            final List<byte[]> readerStateBytes = new ArrayList<>(splitStates.size());
            for (SplitT splitState : splitStates) {
                readerStateBytes.add(splitSerializer.serialize(splitState));
            }
            allStates.put(subtaskId, readerStateBytes);
        }
        return allStates;
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
