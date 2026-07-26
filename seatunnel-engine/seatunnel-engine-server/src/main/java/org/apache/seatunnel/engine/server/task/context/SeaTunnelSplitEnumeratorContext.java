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

package org.apache.seatunnel.engine.server.task.context;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;
import org.apache.seatunnel.engine.server.task.operation.source.AssignSplitOperation;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.common.utils.ExceptionUtil.sneaky;

@Slf4j
public class SeaTunnelSplitEnumeratorContext<SplitT extends SourceSplit>
        implements SourceSplitEnumerator.Context<SplitT> {

    private final int parallelism;

    private final SourceSplitEnumeratorTask<SplitT> task;

    private final MetricsContext metricsContext;
    private final EventListener eventListener;

    private final Set<Integer> noMoreSplitsSignaledReaders = ConcurrentHashMap.newKeySet();

    /** Preserves per-reader split-delivery ordering without blocking the enumerator loop. */
    private final ConcurrentHashMap<Integer, CompletableFuture<Void>> splitDeliveryChains =
            new ConcurrentHashMap<>();

    /** Stores the first asynchronous split-delivery failure for later propagation. */
    private final AtomicReference<IllegalStateException> splitDeliveryFailure =
            new AtomicReference<>();

    public SeaTunnelSplitEnumeratorContext(
            int parallelism,
            SourceSplitEnumeratorTask<SplitT> task,
            MetricsContext metricsContext,
            EventListener eventListener) {
        this.parallelism = parallelism;
        this.task = task;
        this.metricsContext = metricsContext;
        this.eventListener = eventListener;
    }

    @Override
    public int currentParallelism() {
        return parallelism;
    }

    @Override
    public Set<Integer> registeredReaders() {
        return new HashSet<>(task.getRegisteredReaders());
    }

    @Override
    public void assignSplit(int subtaskIndex, List<SplitT> splits) {
        if (registeredReaders().isEmpty()) {
            log.warn("No reader is obtained, skip this assign!");
            return;
        }

        List<byte[]> splitBytes =
                splits.stream()
                        .map(split -> sneaky(() -> task.getSplitSerializer().serialize(split)))
                        .collect(Collectors.toList());
        enqueueSplitDelivery(
                subtaskIndex,
                "assign splits",
                () ->
                        registerSplitDelivery(
                                task.getExecutionContext()
                                        .sendToMember(
                                                new AssignSplitOperation<>(
                                                        task.getTaskMemberLocationByIndex(
                                                                subtaskIndex),
                                                        splitBytes),
                                                task.getTaskMemberAddressByIndex(subtaskIndex)),
                                subtaskIndex,
                                "assign splits"));
    }

    @Override
    public void signalNoMoreSplits(int subtaskIndex) {
        noMoreSplitsSignaledReaders.add(subtaskIndex);
        List<byte[]> emptySplits = Collections.emptyList();
        enqueueSplitDelivery(
                subtaskIndex,
                "signal no more splits",
                () ->
                        registerSplitDelivery(
                                task.getExecutionContext()
                                        .sendToMember(
                                                new AssignSplitOperation<>(
                                                        task.getTaskMemberLocationByIndex(
                                                                subtaskIndex),
                                                        emptySplits),
                                                task.getTaskMemberAddressByIndex(subtaskIndex)),
                                subtaskIndex,
                                "signal no more splits"));
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

    @Override
    public MetricsContext getMetricsContext() {
        return metricsContext;
    }

    @Override
    public EventListener getEventListener() {
        return eventListener;
    }

    public boolean hasNoMoreSplitsSignaled(int subtaskIndex) {
        return noMoreSplitsSignaledReaders.contains(subtaskIndex);
    }

    /**
     * Waits for all split-delivery operations that were already queued before a checkpoint
     * snapshot. This keeps enumerator state snapshots consistent with acknowledged split delivery.
     */
    public void awaitPendingSplitDeliveries() throws InterruptedException, ExecutionException {
        List<CompletableFuture<Void>> pendingDeliveries;
        synchronized (this) {
            pendingDeliveries = new ArrayList<>(splitDeliveryChains.values());
        }
        for (CompletableFuture<Void> splitDeliveryChain : pendingDeliveries) {
            splitDeliveryChain.get();
        }
        throwIfSplitDeliveryFailed();
    }

    /**
     * Surfaces the first asynchronous split-delivery failure on the enumerator task thread so the
     * task can fail instead of remaining stuck in canceling/running state.
     */
    public void throwIfSplitDeliveryFailed() {
        IllegalStateException failure = splitDeliveryFailure.get();
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Chains split-delivery operations per reader so assign/no-more-splits events keep their
     * original ordering without blocking the enumerator task thread.
     */
    private void enqueueSplitDelivery(
            int subtaskIndex,
            String action,
            Supplier<CompletableFuture<Void>> splitDeliverySupplier) {
        // SourceSplitEnumeratorTask holds this same context monitor while taking a checkpoint
        // snapshot, so no delivery can be accepted between the tail snapshot and state snapshot.
        synchronized (this) {
            throwIfSplitDeliveryFailed();
            splitDeliveryChains.compute(
                    subtaskIndex,
                    (ignored, previousDelivery) -> {
                        CompletableFuture<Void> orderedDelivery =
                                previousDelivery == null
                                        ? CompletableFuture.completedFuture(null)
                                        : previousDelivery;
                        return new CompletableFuture<>(
                                orderedDelivery.thenCompose(
                                        unused ->
                                                invokeSplitDelivery(
                                                        splitDeliverySupplier,
                                                        subtaskIndex,
                                                        action)));
                    });
        }
    }

    /**
     * Converts synchronous remote-send failures into the same tracked failure used by asynchronous
     * delivery completion.
     */
    private CompletableFuture<Void> invokeSplitDelivery(
            Supplier<CompletableFuture<Void>> splitDeliverySupplier,
            int subtaskIndex,
            String action) {
        try {
            return splitDeliverySupplier.get();
        } catch (RuntimeException throwable) {
            IllegalStateException failure =
                    recordSplitDeliveryFailure(throwable, subtaskIndex, action);
            CompletableFuture<Void> failedDelivery = new CompletableFuture<>();
            failedDelivery.completeExceptionally(failure);
            return failedDelivery;
        }
    }

    /**
     * Tracks the completion of a remote split-delivery operation so later task loops and
     * checkpoints can observe both completion and failure.
     */
    private CompletableFuture<Void> registerSplitDelivery(
            java.util.concurrent.CompletionStage<?> splitDeliveryStage,
            int subtaskIndex,
            String action) {
        CompletableFuture<Void> splitDeliveryFuture = new CompletableFuture<>();
        splitDeliveryStage.whenComplete(
                (ignored, throwable) -> {
                    if (throwable == null) {
                        splitDeliveryFuture.complete(null);
                        return;
                    }
                    IllegalStateException failure =
                            recordSplitDeliveryFailure(throwable, subtaskIndex, action);
                    splitDeliveryFuture.completeExceptionally(failure);
                });
        return splitDeliveryFuture;
    }

    /**
     * Records and returns the normalized split-delivery failure.
     *
     * @param throwable synchronous or asynchronous delivery failure
     * @param subtaskIndex target reader index
     * @param action delivery action description
     * @return normalized failure retained by the enumerator context
     */
    private IllegalStateException recordSplitDeliveryFailure(
            Throwable throwable, int subtaskIndex, String action) {
        IllegalStateException failure =
                new IllegalStateException(
                        String.format("Failed to %s for reader %s", action, subtaskIndex),
                        unwrapSplitDeliveryThrowable(throwable));
        splitDeliveryFailure.compareAndSet(null, failure);
        return failure;
    }

    /** Unwraps nested async wrappers and exposes the original split-delivery failure cause. */
    private Throwable unwrapSplitDeliveryThrowable(Throwable throwable) {
        Throwable current = throwable;
        while (current instanceof CompletionException || current instanceof ExecutionException) {
            if (current.getCause() == null) {
                break;
            }
            current = current.getCause();
        }
        return current;
    }
}
