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

package org.apache.seatunnel.connectors.seatunnel.common.source.reader;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SplitFetcherManager;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkState;

/**
 * An abstract implementation of {@link SourceReader} which provides some synchronization between
 * the mail box main thread and the SourceReader internal threads. This class allows user to just
 * provide a {@link SplitReader} and snapshot the split state.
 *
 * @param <E> The type of the records (the raw type that typically contains checkpointing
 *     information).
 * @param <T> The final type of the records emitted by the source.
 * @param <SplitT>
 * @param <SplitStateT>
 */
@Slf4j
public abstract class SourceReaderBase<E, T, SplitT extends SourceSplit, SplitStateT>
        implements SourceReader<T, SplitT> {
    private final BlockingQueue<RecordsWithSplitIds<E>> elementsQueue;
    private final ConcurrentMap<String, SplitContext<T, SplitStateT>> splitStates;
    protected final RecordEmitter<E, T, SplitStateT> recordEmitter;
    protected final SplitFetcherManager<E, SplitT> splitFetcherManager;
    protected final SourceReaderOptions options;
    protected final SourceReader.Context context;

    private RecordsWithSplitIds<E> currentFetch;
    protected SplitContext<T, SplitStateT> currentSplitContext;
    private Collector<T> currentSplitOutput;
    @Getter private volatile boolean noMoreSplitsAssignment;

    public SourceReaderBase(
            BlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            SplitFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            SourceReaderOptions options,
            SourceReader.Context context) {
        this.elementsQueue = elementsQueue;
        this.splitFetcherManager = splitFetcherManager;
        this.recordEmitter = recordEmitter;
        this.splitStates = new ConcurrentHashMap<>();
        this.options = options;
        this.context = context;
    }

    @Override
    public void open() {
        log.info("Open Source Reader.");
    }

    @Override
    public void pollNext(Collector<T> output) throws Exception {
        RecordsWithSplitIds<E> recordsWithSplitId = this.currentFetch;
        if (recordsWithSplitId == null) {
            recordsWithSplitId = getNextFetch(output);
            if (recordsWithSplitId == null) {
                if (Boundedness.BOUNDED.equals(context.getBoundedness())
                        && noMoreSplitsAssignment
                        && isNoMoreElement()) {
                    context.signalNoMoreElement();
                    log.info(
                            "Reader {} into idle state, send NoMoreElement event",
                            context.getIndexOfSubtask());
                }
                return;
            }
        }

        E record = recordsWithSplitId.nextRecordFromSplit();
        if (record != null) {
            synchronized (output.getCheckpointLock()) {
                recordEmitter.emitRecord(record, currentSplitOutput, currentSplitContext.state);
            }
            log.trace("Emitted record: {}", record);
        } else if (!moveToNextSplit(recordsWithSplitId, output)) {
            pollNext(output);
        }
    }

    @Override
    public List<SplitT> snapshotState(long checkpointId) {
        List<SplitT> splits = new ArrayList<>();
        splitStates.forEach((id, context) -> splits.add(toSplitType(id, context.state)));
        log.debug("Snapshot state from splits: {}", splits);
        return splits;
    }

    @Override
    public void addSplits(List<SplitT> splits) {
        log.debug("Adding split(s) to reader: {}", splits);
        splits.forEach(
                split -> {
                    // Initialize the state for each split.
                    splitStates.put(
                            split.splitId(),
                            new SplitContext<>(split.splitId(), initializedState(split)));
                });
        splitFetcherManager.addSplits(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader {} received NoMoreSplits event.", context.getIndexOfSubtask());
        noMoreSplitsAssignment = true;
    }

    @Override
    public void handleSourceEvent(SourceEvent sourceEvent) {
        log.info("Received unhandled source event: {}", sourceEvent);
    }

    protected boolean isNoMoreElement() {
        return splitFetcherManager.maybeShutdownFinishedFetchers()
                && elementsQueue.isEmpty()
                && currentFetch == null;
    }

    @Override
    public void close() {
        log.info("Closing Source Reader {}.", context.getIndexOfSubtask());
        try {
            splitFetcherManager.close(options.getSourceReaderCloseTimeout());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private RecordsWithSplitIds<E> getNextFetch(Collector<T> output) {
        splitFetcherManager.checkErrors();
        RecordsWithSplitIds<E> recordsWithSplitId = elementsQueue.poll();
        if (recordsWithSplitId == null || !moveToNextSplit(recordsWithSplitId, output)) {
            try {
                log.trace("Current fetch is finished.");
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new SeaTunnelException(e);
            }
            return null;
        }

        currentFetch = recordsWithSplitId;
        return recordsWithSplitId;
    }

    private boolean moveToNextSplit(
            RecordsWithSplitIds<E> recordsWithSplitIds, Collector<T> output) {
        final String nextSplitId = recordsWithSplitIds.nextSplit();
        if (nextSplitId == null) {
            log.trace("Current fetch is finished.");
            finishCurrentFetch(recordsWithSplitIds, output);
            return false;
        }

        currentSplitContext = splitStates.get(nextSplitId);
        checkState(currentSplitContext != null, "Have records for a split that was not registered");
        currentSplitOutput = currentSplitContext.getOrCreateSplitOutput(output);
        log.trace("Emitting records from fetch for split {}", nextSplitId);
        return true;
    }

    private void finishCurrentFetch(final RecordsWithSplitIds<E> fetch, final Collector<T> output) {
        currentFetch = null;
        currentSplitContext = null;
        currentSplitOutput = null;

        Set<String> finishedSplits = fetch.finishedSplits();
        if (!finishedSplits.isEmpty()) {
            log.info("Finished reading split(s) {}", finishedSplits);
            Map<String, SplitStateT> stateOfFinishedSplits = new HashMap<>();
            for (String finishedSplitId : finishedSplits) {
                stateOfFinishedSplits.put(
                        finishedSplitId, splitStates.remove(finishedSplitId).state);
            }
            onSplitFinished(stateOfFinishedSplits);
        }

        fetch.recycle();
    }

    public int getNumberOfCurrentlyAssignedSplits() {
        return this.splitStates.size();
    }

    /**
     * Handles the finished splits to clean the state if needed.
     *
     * @param finishedSplitIds
     */
    protected abstract void onSplitFinished(Map<String, SplitStateT> finishedSplitIds);

    /**
     * When new splits are added to the reader. The initialize the state of the new splits.
     *
     * @param split a newly added split.
     */
    protected abstract SplitStateT initializedState(SplitT split);

    /**
     * Convert a mutable SplitStateT to immutable SplitT.
     *
     * @param splitState splitState.
     * @return an immutable Split state.
     */
    protected abstract SplitT toSplitType(String splitId, SplitStateT splitState);

    @RequiredArgsConstructor
    protected static final class SplitContext<T, SplitStateT> {
        final String splitId;
        @Getter final SplitStateT state;
        Collector<T> splitOutput;

        Collector<T> getOrCreateSplitOutput(Collector<T> output) {
            if (splitOutput == null) {
                splitOutput = output;
            }
            return splitOutput;
        }
    }
}
