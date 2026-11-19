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

import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Shares one daemon compaction thread across the IMap writers loaded in this JVM. */
@Slf4j
public final class CompactionCoordinator {
    private static ScheduledThreadPoolExecutor executor;
    private static int registrations;

    private CompactionCoordinator() {}

    /**
     * Registers independent input selection and storage implementations with the shared executor.
     */
    public static synchronized Closeable register(
            CompactionPlanner planner, CompactionStorage storage, long intervalSeconds) {
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
        Task task = new Task(planner, storage);
        ScheduledFuture<?> future =
                executor.scheduleWithFixedDelay(
                        task, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        registrations++;
        return new Closeable() {
            private boolean closed;

            @Override
            public synchronized void close() {
                if (closed) {
                    return;
                }
                closed = true;
                future.cancel(false);
                task.close();
                synchronized (CompactionCoordinator.class) {
                    if (--registrations == 0) {
                        executor.shutdown();
                        executor = null;
                    }
                }
            }
        };
    }

    private static final class Task implements Runnable {
        private final CompactionPlanner planner;
        private final CompactionStorage storage;
        private final Set<Path> pendingDeletion = new LinkedHashSet<>();
        private boolean closed;

        private Task(CompactionPlanner planner, CompactionStorage storage) {
            this.planner = planner;
            this.storage = storage;
        }

        /** Only the shared thread performs I/O; writers only add newly sealed input paths. */
        @Override
        public synchronized void run() {
            if (closed) {
                return;
            }
            try {
                deleteReplacedInputs();
                Optional<CompactionPlan> selected = planner.plan();
                if (!selected.isPresent()) {
                    return;
                }
                CompactionPlan plan = selected.get();
                Path snapshot = new WALCompactor().compact(plan, storage);
                planner.committed(plan, snapshot);
                pendingDeletion.addAll(plan.getInputs());
                deleteReplacedInputs();
                log.info("Compacted {} IMap WAL files into {}", plan.getInputs().size(), snapshot);
            } catch (Exception e) {
                // A failed attempt leaves its inputs readable and must not cancel future scheduled
                // attempts.
                log.warn("IMap WAL compaction failed; retained inputs will be retried", e);
            }
        }

        private void deleteReplacedInputs() throws IOException {
            Iterator<Path> inputs = pendingDeletion.iterator();
            while (inputs.hasNext()) {
                storage.delete(inputs.next());
                inputs.remove();
            }
        }

        /** Waits for in-flight compaction before the caller can destroy its storage directory. */
        private synchronized void close() {
            closed = true;
        }
    }
}
