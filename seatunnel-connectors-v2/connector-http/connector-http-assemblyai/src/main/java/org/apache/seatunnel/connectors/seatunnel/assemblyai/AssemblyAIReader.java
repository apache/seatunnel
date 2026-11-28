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
package org.apache.seatunnel.connectors.seatunnel.assemblyai;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Reads each discovered transcript and its selected children through one bounded request worker.
 *
 * <p>Only the reader mutates scan progress. Accepted rows and checkpoint positions advance together
 * under the collector lock, while HTTPS never holds that lock.
 */
final class AssemblyAIReader implements SourceReader<SeaTunnelRow, AssemblyAISplit> {
    private final AssemblyAIConfig config;
    private final Context context;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();

    // Cancellation may run concurrently with an active HTTPS transport or assignment.
    private volatile boolean closed;
    private boolean idle;
    private boolean signaled;
    private AssemblyAISplit progress;
    private AssemblyAIApi.Page page;
    private AssemblyAIApi.Child child;
    private AssemblyAIHttp http;
    private AssemblyAIApi api;
    private ExecutorService executor;
    private Future<?> pending;
    private long expires;

    AssemblyAIReader(AssemblyAIConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Allocates HTTPS only for the active reader.
     *
     * <p>Cancellation before open prevents any later network work.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && executor == null && context.getIndexOfSubtask() == 0) {
                http = new AssemblyAIHttp(config);
                api = new AssemblyAIApi(config, http);
                executor =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "assemblyai-source");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Performs one ordered resource or parent transition per engine poll.
     *
     * <p>Child reads share the parent's validated ID and date scope even if summary output is
     * excluded.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (checkpoint) {
            if (closed || signaled) {
                return;
            }
            if (progress == null ? idle : progress.finished) {
                signaled = true;
                context.signalNoMoreElement();
                return;
            }
            if (progress == null) {
                return;
            }
        }
        if (page == null) {
            page =
                    get(
                            AssemblyAIResource.TRANSCRIPTS,
                            AssemblyAIApi.Page.class,
                            deadline -> api.list(progress.before, progress.previousTail, deadline));
            if (page == null) {
                return;
            }
            verifyPage();
        }
        AssemblyAIApi.Page ready = page;
        if (progress.parent == ready.rows.size()) {
            commit(
                    output,
                    () -> {
                        if (ready.older == null
                                || ready.tail != null && ready.tail.isBefore(progress.start)) {
                            progress.finished = true;
                        } else {
                            progress.nextPage(ready, config.maxPages);
                        }
                        page = null;
                    });
            return;
        }
        AssemblyAIApi.Parent parent = ready.rows.get(progress.parent);
        if (parent.created.isBefore(progress.start)
                || parent.created.isAfter(progress.end)
                || progress.resource == config.resources.size()) {
            commit(
                    output,
                    () -> {
                        progress.pageDigest = ready.digest;
                        progress.nextParent();
                    });
            return;
        }
        AssemblyAIResource resource = config.resources.get(progress.resource);
        if (resource == AssemblyAIResource.TRANSCRIPTS) {
            commit(
                    output,
                    () -> {
                        emit(output, resource, parent.id, parent.id, parent.record);
                        bindParent(ready, parent);
                        progress.nextResource();
                    });
        } else if (!parent.hasChildren()) {
            commit(
                    output,
                    () -> {
                        bindParent(ready, parent);
                        progress.nextResource();
                    });
        } else if (progress.parentId == null) {
            // Commit the chosen parent before any request that a savepoint may interrupt.
            commit(output, () -> bindParent(ready, parent));
        } else {
            emitChild(output, ready, parent, resource);
        }
    }

    private void verifyPage() {
        AssemblyAIResource resource = AssemblyAIResource.TRANSCRIPTS;
        if (progress.parent > page.rows.size()
                || progress.pageDigest != null && !progress.pageDigest.equals(page.digest)
                || progress.parentId != null
                        && (progress.parent == page.rows.size()
                                || !progress.parentId.equals(page.rows.get(progress.parent).id))) {
            throw resource.failure("transcripts page changed since checkpoint");
        }
        if (page.rows.stream().anyMatch(parent -> progress.previousIds.contains(parent.id))) {
            throw resource.failure("older page overlaps the previous transcript page");
        }
    }

    private void emitChild(
            Collector<SeaTunnelRow> output,
            AssemblyAIApi.Page ready,
            AssemblyAIApi.Parent parent,
            AssemblyAIResource resource)
            throws Exception {
        if (child == null) {
            child =
                    get(
                            resource,
                            AssemblyAIApi.Child.class,
                            deadline -> api.child(resource, parent, deadline));
            if (child == null) {
                return;
            }
            if (progress.childRow > child.rows.size()
                    || progress.childDigest != null && !progress.childDigest.equals(child.digest)) {
                throw resource.failure("child rows changed since checkpoint");
            }
        }
        AssemblyAIApi.Child current = child;
        commit(
                output,
                () -> {
                    bindParent(ready, parent);
                    if (progress.childRow < current.rows.size()) {
                        int position = progress.childRow;
                        String id =
                                resource == AssemblyAIResource.REDACTED_AUDIO
                                        ? parent.id
                                        : parent.id + ":" + (position + 1);
                        emit(output, resource, id, parent.id, current.rows.get(position));
                        ++progress.childRow;
                        progress.childDigest = current.digest;
                    }
                    if (progress.childRow == current.rows.size()) {
                        progress.nextResource();
                        child = null;
                    }
                });
    }

    private void bindParent(AssemblyAIApi.Page ready, AssemblyAIApi.Parent parent) {
        progress.pageDigest = ready.digest;
        progress.parentId = parent.id;
    }

    private static void emit(
            Collector<SeaTunnelRow> output,
            AssemblyAIResource resource,
            String id,
            String transcript,
            JsonNode value) {
        SeaTunnelRow row =
                new SeaTunnelRow(new Object[] {resource.label, id, transcript, value.toString()});
        row.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(row);
    }

    private <T> T get(AssemblyAIResource resource, Class<T> kind, Network<T> work)
            throws Exception {
        Future<?> request;
        synchronized (lifecycle) {
            if (closed) {
                return null;
            }
            if (pending == null) {
                expires = http.deadline();
                long deadline = expires;
                Callable<T> task = () -> work.read(deadline);
                pending = executor.submit(task);
            }
            request = pending;
        }
        try {
            T result = kind.cast(request.get(5, TimeUnit.MILLISECONDS));
            synchronized (lifecycle) {
                pending = null;
                return closed ? null : result;
            }
        } catch (TimeoutException ex) {
            if (!closed && expires - System.nanoTime() <= 0) {
                cancel();
                throw resource.failure("HTTPS operation deadline exceeded");
            }
            return null;
        } catch (CancellationException ex) {
            if (closed) {
                return null;
            }
            throw resource.failure("read canceled");
        } catch (ExecutionException ex) {
            if (closed) {
                return null;
            }
            if (ex.getCause() instanceof AssemblyAIResource.Failure) {
                throw (AssemblyAIResource.Failure) ex.getCause();
            }
            throw resource.failure("provider response could not be read");
        }
    }

    private void commit(Collector<SeaTunnelRow> output, Runnable change) {
        synchronized (output.getCheckpointLock()) {
            synchronized (checkpoint) {
                if (!closed) {
                    change.run();
                }
            }
        }
    }

    /**
     * Captures a detached position after the current accepted row.
     *
     * <p>Network bytes in progress and signed media URLs have no checkpoint representation.
     */
    @Override
    public List<AssemblyAISplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return progress == null ? List.of() : List.of(progress.copy());
        }
    }

    /**
     * Accepts one account scan on the active subtask and verifies its logical scope.
     *
     * <p>Restore does not regenerate the original creation bounds.
     */
    @Override
    public void addSplits(List<AssemblyAISplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || progress != null || context.getIndexOfSubtask() != 0) {
                throw new IllegalStateException("Invalid AssemblyAI reader assignment");
            }
            AssemblyAISplit split = splits.get(0);
            split.validate(config);
            progress = split.copy();
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (checkpoint) {
            idle = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /**
     * Aborts current HTTPS and prevents later work on the canceled reader.
     *
     * <p>The source checkpoint lock is independent from this hook.
     */
    @Override
    public void cancel() {
        synchronized (lifecycle) {
            closed = true;
            if (http != null) {
                http.close();
            }
            if (pending != null) {
                pending.cancel(true);
            }
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    @Override
    public void close() {
        cancel();
    }

    /**
     * Carries one bounded remote read and leaves state updates on the engine poll thread.
     *
     * <p>The same absolute deadline applies to all HTTP requests in a child operation.
     */
    private interface Network<T> {
        T read(long deadline) throws InterruptedException;
    }
}
