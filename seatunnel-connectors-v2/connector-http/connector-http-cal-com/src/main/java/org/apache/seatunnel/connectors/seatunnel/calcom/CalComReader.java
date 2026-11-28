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
package org.apache.seatunnel.connectors.seatunnel.calcom;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.calcom.CalComApi.Page;
import org.apache.seatunnel.connectors.seatunnel.calcom.CalComApi.Row;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Drains one Cal.com resource page at a time with checkpointed row progress.
 *
 * <p>Blocking HTTPS runs outside the SeaTunnel poll thread. The reader commits output and row
 * position together under the collector checkpoint lock.
 */
final class CalComReader implements SourceReader<SeaTunnelRow, CalComSplit> {
    private final CalComConfig config;
    private final Context context;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();

    private volatile boolean closed;
    private boolean noMoreSplits;
    private boolean signaled;
    private CalComSplit progress;
    private Page page;

    // The worker has at most one logical request in flight and never modifies scan progress.
    private CalComHttp http;
    private CalComApi api;
    private ExecutorService executor;
    private Future<Page> pending;
    private long expires;

    CalComReader(CalComConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Prepares the transport on the active subtask without issuing a provider request.
     *
     * <p>Cancellation before open remains effective.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && executor == null && context.getIndexOfSubtask() == 0) {
                http = new CalComHttp(config);
                api = new CalComApi(config, http);
                executor =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "calcom-source");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Loads a complete page, advances a cursor, or emits one row.
     *
     * <p>A pending request is checkpointed at its unchanged cursor. No partial response can advance
     * the saved position.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (checkpoint) {
            if (closed || signaled) {
                return;
            }
            if (progress == null ? noMoreSplits : progress.finished(config)) {
                signaled = true;
                context.signalNoMoreElement();
                return;
            }
            if (progress == null) {
                return;
            }
        }
        CalComResource resource = config.resources.get(progress.resourceIndex);
        if (page == null) {
            fetch(output, resource);
        } else if (progress.row == page.rows.size()) {
            commit(
                    output,
                    () -> {
                        if (!page.hasNext) {
                            progress.nextResource();
                        } else {
                            progress.nextPage(page.cursor);
                        }
                        page = null;
                    });
        } else {
            commit(
                    output,
                    () -> {
                        Row row = page.rows.get(progress.row);
                        SeaTunnelRow record =
                                new SeaTunnelRow(
                                        new Object[] {
                                            resource.name, row.id, row.parent, row.record
                                        });
                        record.setTableId(TablePath.DEFAULT.getFullName());
                        output.collect(record);
                        ++progress.row;
                    });
        }
    }

    private void fetch(Collector<SeaTunnelRow> output, CalComResource resource) throws Exception {
        if (progress.digest == null && progress.pages == config.maxPages) {
            throw resource.failure("page count exceeds max_pages_per_resource");
        }
        Page reply = awaitPage(resource, progress.pageIndex() * config.pageSize, progress.cursor);
        if (reply == null) {
            return;
        }
        if (progress.digest != null
                && (!progress.digest.equals(reply.digest) || progress.row > reply.rows.size())) {
            throw resource.failure("checkpoint page changed before replay");
        }
        if (reply.cursor != null
                && progress.cursorHistory.contains(CalComConfig.hash(reply.cursor))) {
            throw resource.failure("repeated nextCursor");
        }
        commit(
                output,
                () -> {
                    if (progress.digest == null) {
                        ++progress.pages;
                    }
                    progress.digest = reply.digest;
                    page = reply;
                });
    }

    private Page awaitPage(CalComResource resource, int offset, String cursor) throws Exception {
        Future<Page> task;
        synchronized (lifecycle) {
            if (closed) {
                return null;
            }
            if (pending == null) {
                expires = http.deadline();
                long deadline = expires;
                pending = executor.submit(() -> api.list(resource, offset, cursor, deadline));
            }
            task = pending;
        }
        try {
            Page reply = task.get(5, TimeUnit.MILLISECONDS);
            synchronized (lifecycle) {
                pending = null;
                return closed ? null : reply;
            }
        } catch (TimeoutException ex) {
            if (!closed && expires - System.nanoTime() <= 0) {
                synchronized (lifecycle) {
                    abortWorker();
                }
                throw resource.failure("HTTPS operation deadline exceeded");
            }
            return null;
        } catch (CancellationException ex) {
            if (closed) {
                return null;
            }
            throw resource.failure("provider read was canceled");
        } catch (ExecutionException ex) {
            if (closed) {
                return null;
            }
            if (ex.getCause() instanceof CalComResource.Failure) {
                throw (CalComResource.Failure) ex.getCause();
            }
            throw resource.failure("provider read failed");
        }
    }

    private void commit(Collector<SeaTunnelRow> output, Runnable action) {
        synchronized (output.getCheckpointLock()) {
            synchronized (checkpoint) {
                if (!closed) {
                    action.run();
                }
            }
        }
    }

    /**
     * Captures the active split without access to response bytes or the token.
     *
     * <p>A restore replays the complete page and verifies its digest before skipping any rows.
     */
    @Override
    public List<CalComSplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return progress == null ? List.of() : List.of(progress.copy());
        }
    }

    /**
     * Validates the recovered scope before accepting a split.
     *
     * <p>Only subtask zero receives the bounded scan split.
     */
    @Override
    public void addSplits(List<CalComSplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || progress != null || context.getIndexOfSubtask() != 0) {
                throw new CalComResource.Failure("checkpoint", "invalid split assignment");
            }
            CalComSplit split = splits.get(0);
            split.validate(config);
            progress = split.copy();
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (checkpoint) {
            noMoreSplits = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /**
     * Cancels the network worker and closes the active socket outside the checkpoint lock.
     *
     * <p>Any in-flight reply becomes ineligible for collection.
     */
    @Override
    public void cancel() {
        synchronized (lifecycle) {
            closed = true;
            abortWorker();
        }
    }

    private void abortWorker() {
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

    @Override
    public void close() {
        cancel();
    }
}
