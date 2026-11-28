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
package org.apache.seatunnel.connectors.seatunnel.aircall;

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

final class AircallReader implements SourceReader<SeaTunnelRow, AircallSplit> {
    private static final int EMIT_PER_POLL = 16;
    private final Context context;
    private final AircallConfig config;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();
    private volatile boolean closed;
    private boolean idle;
    private boolean done;
    private AircallSplit position;

    private AircallHttpTransport transport;
    private ExecutorService requests;
    private volatile Future<AircallPage> pending;
    private long deadline;
    private AircallPage parentPage;
    private AircallPage childPage;

    AircallReader(AircallConfig config, Context context) {
        this.context = context;
        this.config = config;
    }

    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && requests == null && context.getIndexOfSubtask() == 0) {
                transport = new AircallHttpTransport(config);
                requests =
                        Executors.newSingleThreadExecutor(
                                task -> AircallHttpTransport.daemon(task, "aircall-reader"));
            }
        }
    }

    /**
     * Hold at most one root page and one child page while fetching a single request asynchronously.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        AircallConfig.Group group;
        synchronized (checkpoint) {
            if (closed || done) {
                return;
            }
            if (position == null && idle
                    || position != null && position.group == config.groups.size()) {
                done = true;
                context.signalNoMoreElement();
                return;
            }
            if (position == null) {
                return;
            }
            group = config.groups.get(position.group);
        }

        if (parentPage == null) {
            parentPage = fetch(group.root, null, false);
            if (parentPage == null) {
                return;
            }
        }
        if (childPage == null
                && position.parentCommitted
                && position.childIndex < group.children.size()) {
            AircallPage.Row parent = parentPage.rows.get(position.root.index);
            childPage = fetch(group.children.get(position.childIndex), parent.id, true);
            if (childPage == null) {
                return;
            }
        }

        synchronized (output.getCheckpointLock()) {
            synchronized (checkpoint) {
                if (!closed) {
                    collect(output, group);
                }
            }
        }
    }

    private AircallPage fetch(AircallResource resource, String parentId, boolean child)
            throws Exception {
        Future<AircallPage> request = pending;
        if (request == null) {
            AircallSplit snapshot;
            synchronized (checkpoint) {
                snapshot = position.copy();
            }
            AircallSplit.Cursor cursor = child ? snapshot.child : snapshot.root;
            AircallResource.Request target = resource.request(config, snapshot, parentId, cursor);
            long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.deadline);
            synchronized (lifecycle) {
                if (closed) {
                    return null;
                }
                deadline = expires;
                request =
                        requests.submit(
                                () -> transport.page(resource, target, snapshot, cursor, expires));
                pending = request;
            }
        }
        try {
            AircallPage available = request.get(5, TimeUnit.MILLISECONDS);
            pending = null;
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
            if (ex.getCause() instanceof AircallResource.Failure) {
                throw (AircallResource.Failure) ex.getCause();
            }
            throw resource.failure("request could not be completed");
        } catch (CancellationException ex) {
            if (closed) {
                return null;
            }
            throw resource.failure("request canceled");
        }
    }

    private void collect(Collector<SeaTunnelRow> output, AircallConfig.Group group) {
        if (parentPage.saturated) {
            position.narrow(config, group.root);
            parentPage = null;
            return;
        }

        if (group.children.isEmpty()) {
            int stop = Math.min(parentPage.rows.size(), position.root.index + EMIT_PER_POLL);
            while (!closed && position.root.index < stop) {
                emitRoot(output, group, parentPage.rows.get(position.root.index));
                position.root.digest = parentPage.digest;
                position.root.index++;
            }
        } else if (position.root.index < parentPage.rows.size()) {
            AircallPage.Row parent = parentPage.rows.get(position.root.index);
            if (!position.parentCommitted) {
                // Child discovery happens from all parents in range, including roots not exported.
                emitRoot(output, group, parent);
                position.root.digest = parentPage.digest;
                position.parentCommitted = true;
                return;
            }
            if (position.childIndex < group.children.size()) {
                collectChild(output, group.children.get(position.childIndex), parent);
                return;
            }
            position.root.index++;
            position.resetParent();
        }

        if (!closed && position.root.index == parentPage.rows.size()) {
            position.nextRootPage(config, group.root, parentPage);
            parentPage = null;
        }
    }

    private void collectChild(
            Collector<SeaTunnelRow> output, AircallResource resource, AircallPage.Row parent) {
        if (childPage.saturated) {
            throw resource.failure("10,000-result boundary for a child without a date filter");
        }
        AircallSplit.Cursor cursor = position.child;
        int stop = Math.min(childPage.rows.size(), cursor.index + EMIT_PER_POLL);
        while (!closed && cursor.index < stop) {
            emit(
                    output,
                    resource,
                    childPage.rows.get(cursor.index),
                    parent.id,
                    resource.intelligence ? parent.eventEpoch : null);
            cursor.digest = childPage.digest;
            cursor.index++;
        }
        if (!closed && cursor.index == childPage.rows.size()) {
            if (childPage.more) {
                cursor.advance(childPage);
            } else {
                position.childIndex++;
                position.child = new AircallSplit.Cursor();
            }
            childPage = null;
        }
    }

    private void emitRoot(
            Collector<SeaTunnelRow> output, AircallConfig.Group group, AircallPage.Row row) {
        if (!group.exportRoot) {
            return;
        }
        if (group.root == AircallResource.CALLS) {
            // Calls are acquired by creation time. The public call record exposes started_at.
            if (row.eventEpoch < config.first(group.root, position.end)
                    || row.eventEpoch >= position.end) {
                return;
            }
            emit(output, group.root, row, null, row.eventEpoch);
        } else {
            emit(output, group.root, row, null, null);
        }
    }

    private static void emit(
            Collector<SeaTunnelRow> output,
            AircallResource resource,
            AircallPage.Row row,
            String parentId,
            Long callStarted) {
        SeaTunnelRow value =
                new SeaTunnelRow(
                        new Object[] {resource.label(), row.id, parentId, callStarted, row.record});
        value.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(value);
    }

    @Override
    public List<AircallSplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return position == null
                    ? Collections.emptyList()
                    : Collections.singletonList(position.copy());
        }
    }

    @Override
    public void addSplits(List<AircallSplit> splits) {
        synchronized (checkpoint) {
            if (position != null || splits.size() != 1 || context.getIndexOfSubtask() != 0) {
                throw new IllegalStateException("Invalid Aircall reader assignment");
            }
            AircallSplit incoming = splits.get(0);
            config.validate(incoming);
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
            Future<AircallPage> task = pending;
            if (task != null) {
                task.cancel(true);
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
