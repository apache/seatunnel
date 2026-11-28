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
package org.apache.seatunnel.connectors.seatunnel.canny;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.canny.CannyApi.Page;
import org.apache.seatunnel.connectors.seatunnel.canny.CannyApi.Row;
import org.apache.seatunnel.connectors.seatunnel.canny.CannySplit.Frame;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Traverses the Canny workspace and selected board categories.
 *
 * <p>The reader leaves the active parent row in place while visiting children. HTTPS work stays off
 * the poll thread, and row progress moves under the collector checkpoint lock.
 */
final class CannyReader implements SourceReader<SeaTunnelRow, CannySplit> {
    private final CannyConfig config;
    private final Context context;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();
    private final Page[] pages = new Page[2];

    private volatile boolean closed;
    private boolean noMoreSplits;
    private boolean signaled;
    private CannySplit progress;

    // One worker owns the socket and never modifies the traversal stack.
    private CannyHttp http;
    private CannyApi api;
    private ExecutorService executor;
    private Future<Page> pending;
    private long expires;

    CannyReader(CannyConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Prepares the HTTPS worker without contacting Canny.
     *
     * <p>Only the active subtask owns transport state.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && executor == null && context.getIndexOfSubtask() == 0) {
                http = new CannyHttp(config);
                api = new CannyApi(config, http);
                executor =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "canny-source");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Loads a page, emits one row or advances one traversal position.
     *
     * <p>Recovered parents are reloaded from outer to inner before their child request is sent.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (checkpoint) {
            if (closed || signaled) {
                return;
            }
            if (progress == null ? noMoreSplits : progress.finished()) {
                signaled = true;
                context.signalNoMoreElement();
                return;
            }
            if (progress == null) {
                return;
            }
        }
        int last = progress.frames.size() - 1;
        for (int depth = 0; depth <= last; ++depth) {
            if (pages[depth] == null) {
                fetch(output, depth, progress.frames.get(depth));
                return;
            }
        }
        Frame frame = progress.frames.get(last);
        Page page = pages[last];
        if (frame.row == page.rows.size()) {
            commit(output, () -> finishFrame(last, page));
            return;
        }
        Row row = page.rows.get(frame.row);
        List<CannyResource> children =
                frame.resource == CannyResource.BOARDS ? config.children : List.of();
        if (!frame.emitted) {
            commit(
                    output,
                    () -> {
                        if (config.emits(frame.resource)) {
                            emit(output, frame, row);
                        }
                        frame.emitted = true;
                    });
        } else if (frame.child < children.size()) {
            commit(
                    output,
                    () -> {
                        progress.frames.add(new Frame(children.get(frame.child), row.id));
                        ++frame.child;
                    });
        } else {
            commit(output, frame::advanceRow);
        }
    }

    private void fetch(Collector<SeaTunnelRow> output, int depth, Frame frame) throws Exception {
        if (frame.digest == null && progress.acceptedPages == config.scanPages) {
            throw frame.resource.failure("scan exceeds max_pages_per_scan");
        }
        if (depth > 0) {
            Frame parent = progress.frames.get(depth - 1);
            Page parentPage = pages[depth - 1];
            if (parent.row >= parentPage.rows.size()) {
                throw frame.resource.failure("checkpoint parent row is missing");
            }
            if (!Objects.equals(parentPage.rows.get(parent.row).id, frame.boardId)) {
                throw frame.resource.failure("checkpoint board changed before replay");
            }
        }
        Page reply = awaitPage(frame);
        if (reply == null) {
            return;
        }
        if (frame.digest != null
                && (!frame.digest.equals(reply.digest)
                        || frame.row > reply.rows.size()
                        || frame.row == reply.rows.size() && frame.emitted)) {
            throw frame.resource.failure("checkpoint page changed before replay");
        }
        commit(
                output,
                () -> {
                    if (frame.digest == null) {
                        ++progress.acceptedPages;
                    }
                    frame.digest = reply.digest;
                    pages[depth] = reply;
                });
    }

    private void finishFrame(int depth, Page page) {
        int next = page.next;
        Frame frame = progress.frames.get(depth);
        if (next > config.maxPages) {
            throw frame.resource.failure("collection exceeds max_pages_per_collection");
        }
        pages[depth] = null;
        if (next > 0) {
            frame.nextPage(next, page.cursor, page.history);
        } else if (depth == 0) {
            progress.nextRoot(config);
        } else {
            progress.frames.remove(depth);
        }
    }

    private static void emit(Collector<SeaTunnelRow> output, Frame frame, Row row) {
        CannyResource resource = frame.resource;
        SeaTunnelRow emitted =
                new SeaTunnelRow(
                        new Object[] {resource.name, row.id, row.board, row.post, row.record});
        emitted.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(emitted);
    }

    private Page awaitPage(Frame frame) throws Exception {
        CannyResource resource = frame.resource;
        Future<Page> task;
        synchronized (lifecycle) {
            if (closed) {
                return null;
            }
            if (pending == null) {
                expires = http.deadline();
                long deadline = expires;
                pending = executor.submit(() -> api.list(frame, deadline));
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
            if (ex.getCause() instanceof CannyResource.Failure) {
                throw (CannyResource.Failure) ex.getCause();
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
     * Captures the active traversal without saving response bytes or the token.
     *
     * <p>A restore verifies each pinned parent and target page before continuing.
     */
    @Override
    public List<CannySplit> snapshotState(long checkpointId) {
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
    public void addSplits(List<CannySplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || progress != null || context.getIndexOfSubtask() != 0) {
                throw new CannyResource.Failure("checkpoint", "invalid split assignment");
            }
            CannySplit split = splits.get(0);
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
