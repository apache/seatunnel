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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.event.EnumeratorCloseEvent;
import org.apache.seatunnel.api.source.event.EnumeratorOpenEvent;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The implementation of {@link SplitEnumerator}, used for proxy all {@link SourceSplitEnumerator}
 * in flink.
 *
 * @param <SplitT> The generic type of source split
 * @param <EnumStateT> The generic type of enumerator state
 */
public class FlinkSourceEnumerator<SplitT extends SourceSplit, EnumStateT>
        implements SplitEnumerator<SplitWrapper<SplitT>, EnumStateT> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkSourceEnumerator.class);

    private final SourceSplitEnumerator<SplitT, EnumStateT> sourceSplitEnumerator;

    private final SplitEnumeratorContext<SplitWrapper<SplitT>> enumeratorContext;

    private final SourceSplitEnumerator.Context<SplitT> context;
    private final int parallelism;
    private final Set<Integer> noMoreSplitsSignaledReaders;

    private final Object lock = new Object();
    private final Set<Integer> registeredReaderIds = new HashSet<>();

    private volatile boolean isRun = false;
    private volatile boolean isRunning = false;

    public FlinkSourceEnumerator(
            SourceSplitEnumerator<SplitT, EnumStateT> enumerator,
            SplitEnumeratorContext<SplitWrapper<SplitT>> enumContext,
            Set<Integer> noMoreSplitsSignaledReaders) {
        this.sourceSplitEnumerator = enumerator;
        this.enumeratorContext = enumContext;
        this.context = new FlinkSourceSplitEnumeratorContext<>(enumeratorContext);
        this.parallelism = enumeratorContext.currentParallelism();
        this.noMoreSplitsSignaledReaders = noMoreSplitsSignaledReaders;
    }

    @Override
    public void start() {
        sourceSplitEnumerator.open();
        context.getEventListener().onEvent(new EnumeratorOpenEvent());
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        sourceSplitEnumerator.handleSplitRequest(subtaskId);
    }

    @Override
    public void addSplitsBack(List<SplitWrapper<SplitT>> splits, int subtaskId) {
        synchronized (lock) {
            sourceSplitEnumerator.addSplitsBack(
                    splits.stream().map(SplitWrapper::getSourceSplit).collect(Collectors.toList()),
                    subtaskId);
        }
    }

    @Override
    public void addReader(int subtaskId) {
        boolean needResignalNoMoreSplits;
        boolean shouldRun = false;
        synchronized (lock) {
            sourceSplitEnumerator.registerReader(subtaskId);
            registeredReaderIds.add(subtaskId);
            needResignalNoMoreSplits = noMoreSplitsSignaledReaders.contains(subtaskId);
            if (!isRun && !isRunning && registeredReaderIds.size() == parallelism) {
                shouldRun = true;
                isRunning = true;
            }
        }
        if (shouldRun) {
            try {
                sourceSplitEnumerator.run();
                synchronized (lock) {
                    isRun = true;
                    isRunning = false;
                }
            } catch (Exception e) {
                synchronized (lock) {
                    isRunning = false;
                }
                throw new RuntimeException(e);
            }
        }
        if (needResignalNoMoreSplits) {
            LOGGER.info(
                    "Reader [{}] re-registered after failover. Re-signaling NoMoreSplitsEvent.",
                    subtaskId);
            enumeratorContext.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public EnumStateT snapshotState(long checkpointId) throws Exception {
        synchronized (lock) {
            return sourceSplitEnumerator.snapshotState(checkpointId);
        }
    }

    @Override
    public void close() throws IOException {
        sourceSplitEnumerator.close();
        context.getEventListener().onEvent(new EnumeratorCloseEvent());
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof NoMoreElementEvent) {
            LOGGER.info(
                    "Received NoMoreElementEvent from reader [{}], total registered readers [{}]",
                    subtaskId,
                    enumeratorContext.currentParallelism());
            enumeratorContext.sendEventToSourceReader(subtaskId, sourceEvent);
        }
        if (sourceEvent instanceof SourceEventWrapper) {
            sourceSplitEnumerator.handleSourceEvent(
                    subtaskId, (((SourceEventWrapper) sourceEvent).getSourceEvent()));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        sourceSplitEnumerator.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        sourceSplitEnumerator.notifyCheckpointAborted(checkpointId);
    }
}
