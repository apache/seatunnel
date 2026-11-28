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
package org.apache.seatunnel.connectors.seatunnel.algolia;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Drives bounded parent and child requests without holding the engine checkpoint lock during I/O.
 */
final class AlgoliaReader implements SourceReader<SeaTunnelRow, AlgoliaSplit> {
    private static final int EMIT_PER_POLL = 16;
    private final Context context;
    private final AlgoliaConfig config;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();
    private volatile boolean closed;
    private boolean idle;
    private boolean done;
    private AlgoliaSplit position;
    private AlgoliaHttpTransport transport;
    private ExecutorService requests;
    private Future<AlgoliaPage> pending;
    private long deadline;
    private AlgoliaPage rootPage;
    private AlgoliaPage childPage;

    AlgoliaReader(AlgoliaConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && requests == null && context.getIndexOfSubtask() == 0) {
                transport = new AlgoliaHttpTransport(config);
                requests =
                        Executors.newSingleThreadExecutor(
                                task -> AlgoliaHttpTransport.daemon(task, "algolia-reader"));
            }
        }
    }

    /** Load at most two pages, and keep network I/O outside the checkpoint lock. */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        AlgoliaResource resource;
        synchronized (checkpoint) {
            if (closed || done) {
                return;
            }
            if (position == null && idle
                    || position != null && position.group == config.roots.size()) {
                done = true;
                context.signalNoMoreElement();
                return;
            }
            if (position == null) {
                return;
            }
            resource = config.roots.get(position.group);
        }
        boolean nested = resource == AlgoliaResource.INDICES && !config.children.isEmpty();
        if (rootPage == null) {
            rootPage = fetch(resource, null, position.root);
            if (rootPage == null) {
                return;
            }
            verifyResume(resource, rootPage, position.root);
            if (nested
                    && (position.parent > rootPage.rows.size()
                            || position.indexEmitted && position.parent == rootPage.rows.size()
                            || position.parentPageDigest != null
                                    && !position.parentPageDigest.equals(rootPage.digest))) {
                throw resource.failure("saved parent page has changed");
            }
        }
        if (nested
                && position.indexEmitted
                && position.childIndex < config.children.size()
                && childPage == null) {
            AlgoliaResource childResource = config.children.get(position.childIndex);
            childPage = fetch(childResource, rootPage.rows.get(position.parent).id, position.child);
            if (childPage == null) {
                return;
            }
            verifyResume(childResource, childPage, position.child);
        }
        synchronized (output.getCheckpointLock()) {
            synchronized (checkpoint) {
                if (closed) {
                    return;
                }
                if (nested) {
                    collectIndex(output);
                    if (position.parent < rootPage.rows.size()) {
                        return;
                    }
                } else {
                    emitRows(output, resource, rootPage, position.root, null);
                    if (position.root.row < rootPage.rows.size()) {
                        return;
                    }
                }
                if (closed) {
                    return;
                }
                if (rootPage.more) {
                    position.root.advance(resource, rootPage);
                    position.clearParentPage();
                } else {
                    position.nextGroup();
                }
                rootPage = null;
            }
        }
    }

    private AlgoliaPage fetch(AlgoliaResource resource, String family, AlgoliaSplit.Cursor page)
            throws Exception {
        Future<AlgoliaPage> request;
        synchronized (lifecycle) {
            if (closed) {
                return null;
            }
            if (pending == null) {
                long expires =
                        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.pageDeadline);
                deadline = expires;
                pending = requests.submit(() -> transport.page(resource, family, page, expires));
            }
            request = pending;
        }
        try {
            AlgoliaPage available = request.get(5, TimeUnit.MILLISECONDS);
            synchronized (lifecycle) {
                pending = null;
            }
            return closed ? null : available;
        } catch (TimeoutException ex) {
            if (!closed && deadline - System.nanoTime() <= 0) {
                cancel();
                throw resource.failure("page deadline exceeded");
            }
            return null;
        } catch (ExecutionException ex) {
            if (closed) {
                return null;
            }
            if (ex.getCause() instanceof AlgoliaResource.Failure) {
                throw (AlgoliaResource.Failure) ex.getCause();
            }
            throw resource.failure("request could not be completed");
        } catch (CancellationException ex) {
            if (closed) {
                return null;
            }
            throw resource.failure("request canceled");
        }
    }

    private static void verifyResume(
            AlgoliaResource resource, AlgoliaPage page, AlgoliaSplit.Cursor saved) {
        if (saved.row > page.rows.size()
                || saved.activeDigest != null && !saved.activeDigest.equals(page.digest)) {
            throw resource.failure("saved page has changed");
        }
    }

    private void collectIndex(Collector<SeaTunnelRow> output) {
        if (position.parent == rootPage.rows.size()) {
            return;
        }
        AlgoliaPage.Row index = rootPage.rows.get(position.parent);
        if (!position.indexEmitted) {
            // Old index rows can be filtered out while their children are still selected.
            if (config.selection.contains(AlgoliaResource.INDICES)) {
                emit(output, AlgoliaResource.INDICES, index, null);
            }
            position.parentPageDigest = rootPage.digest;
            position.indexEmitted = true;
            return;
        }
        if (position.childIndex == config.children.size()) {
            position.nextParent();
            return;
        }
        AlgoliaResource resource = config.children.get(position.childIndex);
        emitRows(output, resource, childPage, position.child, index.id);
        if (!closed && position.child.row == childPage.rows.size()) {
            if (childPage.more) {
                position.child.advance(resource, childPage);
            } else {
                position.childIndex++;
                position.child = new AlgoliaSplit.Cursor();
            }
            childPage = null;
        }
    }

    private void emitRows(
            Collector<SeaTunnelRow> output,
            AlgoliaResource resource,
            AlgoliaPage page,
            AlgoliaSplit.Cursor state,
            String family) {
        int stop = Math.min(page.rows.size(), state.row + EMIT_PER_POLL);
        while (!closed && state.row < stop) {
            emit(output, resource, page.rows.get(state.row), family);
            state.activeDigest = page.digest;
            state.row++;
        }
    }

    private void emit(
            Collector<SeaTunnelRow> output,
            AlgoliaResource resource,
            AlgoliaPage.Row row,
            String family) {
        if (row.timestamp != null
                && (row.timestamp.isBefore(config.start)
                        || !row.timestamp.isBefore(position.end))) {
            return;
        }
        SeaTunnelRow value =
                new SeaTunnelRow(new Object[] {resource.optionName(), row.id, family, row.record});
        value.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(value);
    }

    @Override
    public List<AlgoliaSplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return position == null
                    ? Collections.emptyList()
                    : Collections.singletonList(position.copy());
        }
    }

    @Override
    public void addSplits(List<AlgoliaSplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (position != null || splits.size() != 1 || context.getIndexOfSubtask() != 0) {
                throw new IllegalStateException("Invalid Algolia reader assignment");
            }
            AlgoliaSplit incoming = splits.get(0);
            incoming.validate(config);
            position = incoming.copy();
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

    @Override
    public void cancel() {
        synchronized (lifecycle) {
            closed = true;
            if (transport != null) {
                transport.close();
            }
            if (pending != null) {
                pending.cancel(true);
            }
            if (requests != null) {
                requests.shutdownNow();
            }
        }
    }

    @Override
    public void close() {
        cancel();
    }
}
