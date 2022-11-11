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

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

@Slf4j
public class SplitFetcher<E, SplitT extends SourceSplit> implements Runnable {
    @Getter
    private final int fetcherId;
    private final Deque<SplitFetcherTask> taskQueue = new ArrayDeque<>();
    @Getter
    private final Map<String, SplitT> assignedSplits = new HashMap<>();
    @Getter
    private final SplitReader<E, SplitT> splitReader;
    private final Consumer<Throwable> errorHandler;
    private final Runnable shutdownHook;
    private final FetchTask fetchTask;

    private volatile boolean closed;
    private volatile SplitFetcherTask runningTask = null;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition nonEmpty = lock.newCondition();

    SplitFetcher(int fetcherId,
                 @NonNull BlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
                 @NonNull SplitReader<E, SplitT> splitReader,
                 @NonNull Consumer<Throwable> errorHandler,
                 @NonNull Runnable shutdownHook,
                 @NonNull Consumer<Collection<String>> splitFinishedHook) {
        this.fetcherId = fetcherId;
        this.splitReader = splitReader;
        this.errorHandler = errorHandler;
        this.shutdownHook = shutdownHook;
        this.fetchTask = new FetchTask<>(
            splitReader,
            elementsQueue,
            finishedSplits -> {
                finishedSplits.forEach(assignedSplits::remove);
                splitFinishedHook.accept(finishedSplits);
                log.info("Finished reading from splits {}", finishedSplits);
            },
            fetcherId);
    }

    @Override
    public void run() {
        log.info("Starting split fetcher {}", fetcherId);
        try {
            while (runOnce()) {
                // nothing to do, everything is inside #runOnce.
            }
        } catch (Throwable t) {
            errorHandler.accept(t);
        } finally {
            try {
                splitReader.close();
            } catch (Exception e) {
                errorHandler.accept(e);
            } finally {
                log.info("Split fetcher {} exited.", fetcherId);
                shutdownHook.run();
            }
        }
    }

    public void addSplits(@NonNull Collection<SplitT> splitsToAdd) {
        lock.lock();
        try {
            addTaskUnsafe(new AddSplitsTask<>(splitReader, splitsToAdd, assignedSplits));
            wakeUpUnsafe(true);
        } finally {
            lock.unlock();
        }
    }

    public void addTask(@NonNull SplitFetcherTask task) {
        lock.lock();
        try {
            addTaskUnsafe(task);
        } finally {
            lock.unlock();
        }
    }

    public void shutdown() {
        lock.lock();
        try {
            if (!closed) {
                closed = true;
                log.info("Shutting down split fetcher {}", fetcherId);
                wakeUpUnsafe(false);
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean isIdle() {
        lock.lock();
        try {
            return assignedSplits.isEmpty() && taskQueue.isEmpty() && runningTask == null;
        } finally {
            lock.unlock();
        }
    }

    private boolean runOnce() {
        lock.lock();
        SplitFetcherTask nextTask;
        try {
            if (closed) {
                return false;
            }

            nextTask = getNextTaskUnsafe();
            if (nextTask == null) {
                // (spurious) wakeup, so just repeat
                return true;
            }

            log.debug("Prepare to run {}", nextTask);
            // store task for #wakeUp
            this.runningTask = nextTask;
        } finally {
            lock.unlock();
        }

        // execute the task outside of lock, so that it can be woken up
        try {
            nextTask.run();
        } catch (Exception e) {
            throw new RuntimeException(
                String.format("SplitFetcher thread %d received unexpected exception while polling the records",
                    fetcherId), e);
        }

        // re-acquire lock as all post-processing steps, need it
        lock.lock();
        try {
            this.runningTask = null;
        } finally {
            lock.unlock();
        }
        return true;
    }

    private SplitFetcherTask getNextTaskUnsafe() {
        assert lock.isHeldByCurrentThread();

        try {
            if (!taskQueue.isEmpty()) {
                // execute tasks in taskQueue first
                return taskQueue.poll();
            } else if (!assignedSplits.isEmpty()) {
                // use fallback task = fetch if there is at least one split
                return fetchTask;
            } else {
                // nothing to do, wait for signal
                nonEmpty.await();
                return taskQueue.poll();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("The thread was interrupted while waiting for a fetcher task.");
        }
    }

    private void wakeUpUnsafe(boolean taskOnly) {
        assert lock.isHeldByCurrentThread();

        SplitFetcherTask currentTask = runningTask;
        if (currentTask != null) {
            log.debug("Waking up running task {}", currentTask);
            currentTask.wakeUp();
        } else if (!taskOnly) {
            log.debug("Waking up fetcher thread.");
            nonEmpty.signal();
        }
    }

    private void addTaskUnsafe(SplitFetcherTask task) {
        assert lock.isHeldByCurrentThread();

        taskQueue.add(task);
        nonEmpty.signal();
    }
}
