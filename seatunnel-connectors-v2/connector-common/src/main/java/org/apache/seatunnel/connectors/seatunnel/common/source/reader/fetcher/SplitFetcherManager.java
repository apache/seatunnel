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

package org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;

import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The split fetcher manager could be used to support different threading models by implementing
 * the {@link #addSplits(Collection)} method differently. For example, a single thread split fetcher
 * manager would only start a single fetcher and assign all the splits to it. A one-thread-per-split
 * fetcher may spawn a new thread every time a new split is assigned.
 *
 * @param <E>
 *
 * @param <SplitT>
 *
 */
@Slf4j
public abstract class SplitFetcherManager<E, SplitT extends SourceSplit> {
    protected final Map<Integer, SplitFetcher<E, SplitT>> fetchers;
    private final BlockingQueue<RecordsWithSplitIds<E>> elementsQueue;
    private final Supplier<SplitReader<E, SplitT>> splitReaderFactory;
    private final Consumer<Collection<String>> splitFinishedHook;
    private final AtomicInteger fetcherIdGenerator;
    private final AtomicReference<Throwable> uncaughtFetcherException;
    private final Consumer<Throwable> errorHandler;
    private final ExecutorService executors;
    private volatile boolean closed;

    public SplitFetcherManager(BlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
                               Supplier<SplitReader<E, SplitT>> splitReaderFactory) {
        this(elementsQueue, splitReaderFactory, ignore -> {});
    }

    public SplitFetcherManager(BlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
                               Supplier<SplitReader<E, SplitT>> splitReaderFactory,
                               Consumer<Collection<String>> splitFinishedHook) {
        this.fetchers = new ConcurrentHashMap<>();
        this.elementsQueue = elementsQueue;
        this.splitReaderFactory = splitReaderFactory;
        this.splitFinishedHook = splitFinishedHook;
        this.fetcherIdGenerator = new AtomicInteger(0);
        this.uncaughtFetcherException = new AtomicReference<>(null);
        this.errorHandler = throwable -> {
            log.error("Received uncaught exception.", throwable);
            if (!uncaughtFetcherException.compareAndSet(null, throwable)) {
                // Add the exception to the exception list.
                uncaughtFetcherException.get().addSuppressed(throwable);
            }
        };
        String taskThreadName = Thread.currentThread().getName();
        this.executors = Executors.newCachedThreadPool(
            r -> new Thread(r, "Source Data Fetcher for " + taskThreadName));
    }

    public abstract void addSplits(Collection<SplitT> splitsToAdd);

    protected void startFetcher(SplitFetcher<E, SplitT> fetcher) {
        executors.submit(fetcher);
    }

    protected synchronized SplitFetcher<E, SplitT> createSplitFetcher() {
        if (closed) {
            throw new IllegalStateException("The split fetcher manager has closed.");
        }
        // Create SplitReader.
        SplitReader<E, SplitT> splitReader = splitReaderFactory.get();
        int fetcherId = fetcherIdGenerator.getAndIncrement();
        SplitFetcher<E, SplitT> splitFetcher =
            new SplitFetcher<>(
                fetcherId,
                elementsQueue,
                splitReader,
                errorHandler,
                () -> {
                    fetchers.remove(fetcherId);
                },
                this.splitFinishedHook);
        fetchers.put(fetcherId, splitFetcher);
        return splitFetcher;
    }

    public synchronized boolean maybeShutdownFinishedFetchers() {
        Iterator<Map.Entry<Integer, SplitFetcher<E, SplitT>>> iter = fetchers.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer, SplitFetcher<E, SplitT>> entry = iter.next();
            SplitFetcher<E, SplitT> fetcher = entry.getValue();
            if (fetcher.isIdle()) {
                log.info("Closing splitFetcher {} because it is idle.", entry.getKey());
                fetcher.shutdown();
                iter.remove();
            }
        }
        return fetchers.isEmpty();
    }

    public synchronized void close(long timeoutMs) throws Exception {
        closed = true;
        fetchers.values().forEach(SplitFetcher::shutdown);
        executors.shutdown();
        if (!executors.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
            log.warn("Failed to close the source reader in {} ms. There are still {} split fetchers running",
                timeoutMs,
                fetchers.size());
        }
    }

    public void checkErrors() {
        if (uncaughtFetcherException.get() != null) {
            throw new RuntimeException("One or more fetchers have encountered exception",
                uncaughtFetcherException.get());
        }
    }
}
