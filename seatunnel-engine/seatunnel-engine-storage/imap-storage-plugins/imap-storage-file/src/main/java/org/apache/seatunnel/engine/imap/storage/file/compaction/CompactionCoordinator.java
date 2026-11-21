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

package org.apache.seatunnel.engine.imap.storage.file.compaction;

import org.apache.seatunnel.engine.imap.storage.file.common.WALCompactor;
import org.apache.seatunnel.engine.imap.storage.file.config.CompactionConfig;

import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Shares one daemon compaction thread across the IMap writers loaded by this class loader. */
@Slf4j
public final class CompactionCoordinator {
    private static final long STOP_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);
    private static ScheduledThreadPoolExecutor executor;
    private static int registrations;
    private static int activeRuns;

    private CompactionCoordinator() {}

    /** Registers a writer with the default compaction working-set limits. */
    public static Closeable register(
            CompactionPlanner planner, CompactionStorage storage, long intervalSeconds) {
        return register(
                planner,
                storage,
                intervalSeconds,
                CompactionConfig.MAX_MEMORY_BYTES.defaultValue(),
                CompactionConfig.MAX_KEYS.defaultValue());
    }

    /** Registers independent input selection and storage with the shared executor. */
    public static Closeable register(
            CompactionPlanner planner,
            CompactionStorage storage,
            long intervalSeconds,
            long maxMemoryBytes,
            long maxKeys) {
        return register(
                planner, storage, intervalSeconds, maxMemoryBytes, maxKeys, STOP_TIMEOUT_MILLIS);
    }

    static synchronized Closeable register(
            CompactionPlanner planner,
            CompactionStorage storage,
            long intervalSeconds,
            long maxMemoryBytes,
            long maxKeys,
            long stopTimeoutMillis) {
        if (executor == null) {
            executor =
                    new ScheduledThreadPoolExecutor(
                            1,
                            runnable -> {
                                Thread thread = new Thread(runnable, "seatunnel-imap-compaction");
                                thread.setDaemon(true);
                                thread.setContextClassLoader(
                                        CompactionCoordinator.class.getClassLoader());
                                return thread;
                            });
            executor.setRemoveOnCancelPolicy(true);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        }
        Task task = new Task(planner, storage, maxMemoryBytes, maxKeys);
        ScheduledFuture<?> future =
                executor.scheduleWithFixedDelay(
                        task, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        registrations++;
        return new Closeable() {
            private boolean closed;

            /**
             * Requests cancellation before waiting at most once. A timeout never means the task is
             * safe to delete: later calls continue to fail until the in-flight I/O actually exits.
             */
            @Override
            public synchronized void close() throws IOException {
                boolean firstClose = !closed;
                if (firstClose) {
                    closed = true;
                    task.requestStop();
                    future.cancel(true);
                    synchronized (CompactionCoordinator.class) {
                        registrations--;
                        shutdownIfIdle();
                    }
                }
                if (!task.awaitStopped(firstClose ? stopTimeoutMillis : 0)) {
                    throw new IOException(
                            "IMap compaction did not stop within "
                                    + stopTimeoutMillis
                                    + " ms; retain its storage directory until the in-flight I/O exits");
                }
            }
        };
    }

    /**
     * A blocked, cancelled task keeps the existing executor until it exits. New registrations then
     * share that thread instead of starting a second compactor alongside the in-flight operation.
     */
    private static void shutdownIfIdle() {
        if (registrations == 0 && activeRuns == 0 && executor != null) {
            executor.shutdown();
            executor = null;
        }
    }

    private static final class Task implements Runnable {
        private final CompactionPlanner planner;
        private final CompactionStorage storage;
        private final long maxMemoryBytes;
        private final long maxKeys;
        private final Set<Path> pendingDeletion = new LinkedHashSet<>();
        private final Object lifecycle = new Object();
        private volatile boolean closed;
        private boolean running;
        private boolean failed;
        private CompactionPlan deferredPlan;

        private Task(
                CompactionPlanner planner,
                CompactionStorage storage,
                long maxMemoryBytes,
                long maxKeys) {
            this.planner = planner;
            this.storage = storage;
            this.maxMemoryBytes = maxMemoryBytes;
            this.maxKeys = maxKeys;
        }

        /** Only the shared thread performs I/O; the lifecycle monitor never covers that I/O. */
        @Override
        public void run() {
            synchronized (lifecycle) {
                if (closed || failed) {
                    return;
                }
                running = true;
                synchronized (CompactionCoordinator.class) {
                    activeRuns++;
                }
            }
            CompactionPlan plan = null;
            try {
                checkRunning();
                deleteReplacedInputs();
                Optional<CompactionPlan> selected = planner.plan();
                if (!selected.isPresent()) {
                    return;
                }
                plan = selected.get();
                if (samePlan(plan, deferredPlan)) {
                    return;
                }
                Path snapshot =
                        new WALCompactor()
                                .compact(plan, storage, maxMemoryBytes, maxKeys, () -> closed);
                checkRunning();
                planner.committed(plan, snapshot);
                deferredPlan = null;
                pendingDeletion.addAll(plan.getInputs());
                deleteReplacedInputs();
                log.info("Compacted {} IMap WAL files into {}", plan.getInputs().size(), snapshot);
            } catch (CompactionDeferredException e) {
                // Immutable, unchanged inputs cannot resolve a capacity limit or an ambiguous TTL.
                // Retry only after the planner selects a changed file set, without repeated scans.
                deferredPlan = plan;
                if (!closed) {
                    log.warn(
                            "IMap WAL compaction deferred under {}; retained inputs will be retried "
                                    + "when the plan changes: {}",
                            plan == null ? planner : plan.getOutputDirectory(),
                            e.getMessage());
                }
            } catch (Exception e) {
                if (!closed) {
                    log.warn(
                            "IMap WAL compaction failed under {}; retained inputs will be retried",
                            plan == null ? planner : plan.getOutputDirectory(),
                            e);
                }
            } catch (Error e) {
                // Continuing after an OOM or another VM error is not a safe retry policy. Make the
                // disabled task visible instead of leaving its exceptional future unobserved.
                failed = true;
                log.error(
                        "IMap WAL compaction disabled under {} after a fatal error; inputs are "
                                + "retained. Reinitialize the storage after resolving the failure",
                        plan == null ? planner : plan.getOutputDirectory(),
                        e);
                throw e;
            } finally {
                synchronized (lifecycle) {
                    running = false;
                    lifecycle.notifyAll();
                }
                synchronized (CompactionCoordinator.class) {
                    activeRuns--;
                    shutdownIfIdle();
                }
            }
        }

        private boolean samePlan(CompactionPlan left, CompactionPlan right) {
            return right != null
                    && left.getOutputDirectory().equals(right.getOutputDirectory())
                    && left.getInputs().equals(right.getInputs());
        }

        private void checkRunning() throws InterruptedIOException {
            if (closed || Thread.currentThread().isInterrupted()) {
                throw new InterruptedIOException("IMap compaction is stopping");
            }
        }

        private void deleteReplacedInputs() throws IOException {
            Iterator<Path> inputs = pendingDeletion.iterator();
            while (inputs.hasNext()) {
                checkRunning();
                storage.delete(inputs.next());
                inputs.remove();
            }
        }

        private void requestStop() {
            synchronized (lifecycle) {
                closed = true;
            }
        }

        private boolean awaitStopped(long timeoutMillis) throws IOException {
            long remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            long deadline = System.nanoTime() + remainingNanos;
            synchronized (lifecycle) {
                while (running && remainingNanos > 0) {
                    try {
                        TimeUnit.NANOSECONDS.timedWait(lifecycle, remainingNanos);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new InterruptedIOException(
                                "Interrupted while stopping IMap compaction");
                    }
                    remainingNanos = deadline - System.nanoTime();
                }
                return !running;
            }
        }
    }
}
