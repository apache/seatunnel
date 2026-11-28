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
package org.apache.seatunnel.connectors.seatunnel.akeneo;

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

final class AkeneoReader implements SourceReader<SeaTunnelRow, AkeneoSplit> {
    private static final int EMIT_PER_POLL = 16;
    private final Context context;
    private final AkeneoConfig config;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();
    private volatile boolean closed;
    private boolean idle;
    private boolean done;
    private AkeneoSplit position;
    private AkeneoHttpTransport transport;
    private ExecutorService requests;
    private Future<AkeneoPage> pending;
    private long deadline;
    private AkeneoPage rootPage;
    private AkeneoPage childPage;

    AkeneoReader(AkeneoConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && requests == null && context.getIndexOfSubtask() == 0) {
                transport = new AkeneoHttpTransport(config);
                requests =
                        Executors.newSingleThreadExecutor(
                                task -> AkeneoHttpTransport.daemon(task, "akeneo-reader"));
            }
        }
    }

    /** Load at most two pages, and keep network I/O outside the checkpoint lock. */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        AkeneoResource resource;
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
            resource = config.groups.get(position.group);
        }
        boolean variants =
                resource == AkeneoResource.FAMILIES
                        && config.selection.contains(AkeneoResource.FAMILY_VARIANTS);
        if (rootPage == null) {
            rootPage = fetch(resource, null, position.root);
            if (rootPage == null) {
                return;
            }
            verifyResume(resource, rootPage, position.root);
            if (variants
                    && (position.parent > rootPage.rows.size()
                            || position.familyEmitted && position.parent == rootPage.rows.size()
                            || position.familyPageDigest != null
                                    && !position.familyPageDigest.equals(rootPage.digest))) {
                throw resource.failure("saved parent page has changed");
            }
        }
        if (variants && position.familyEmitted && childPage == null) {
            AkeneoPage.Row family = rootPage.rows.get(position.parent);
            childPage = fetch(AkeneoResource.FAMILY_VARIANTS, family.id, position.child);
            if (childPage == null) {
                return;
            }
            verifyResume(AkeneoResource.FAMILY_VARIANTS, childPage, position.child);
        }
        synchronized (output.getCheckpointLock()) {
            synchronized (checkpoint) {
                if (closed) {
                    return;
                }
                if (variants) {
                    collectFamily(output);
                    if (position.parent < rootPage.rows.size()) {
                        return;
                    }
                } else {
                    emitRows(output, resource, rootPage, position.root, null);
                    if (position.root.offset < rootPage.rows.size()) {
                        return;
                    }
                }
                if (closed) {
                    return;
                }
                if (rootPage.more) {
                    position.root.advance(resource, rootPage.nextCursor, config.maxPages);
                    position.clearFamilyPage();
                } else {
                    position.nextGroup();
                }
                rootPage = null;
            }
        }
    }

    private AkeneoPage fetch(AkeneoResource resource, String family, AkeneoSplit.Position page)
            throws Exception {
        Future<AkeneoPage> request;
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
            AkeneoPage available = request.get(5, TimeUnit.MILLISECONDS);
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
            if (ex.getCause() instanceof AkeneoResource.Failure) {
                throw (AkeneoResource.Failure) ex.getCause();
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
            AkeneoResource resource, AkeneoPage page, AkeneoSplit.Position saved) {
        if (saved.offset > page.rows.size()
                || saved.prefix != null && !saved.prefix.equals(page.digest)) {
            throw resource.failure("saved page has changed");
        }
    }

    private void collectFamily(Collector<SeaTunnelRow> output) {
        if (position.parent == rootPage.rows.size()) {
            return;
        }
        AkeneoPage.Row family = rootPage.rows.get(position.parent);
        if (!position.familyEmitted) {
            if (config.selection.contains(AkeneoResource.FAMILIES)) {
                emit(output, AkeneoResource.FAMILIES, family, null);
            }
            position.familyPageDigest = rootPage.digest;
            position.familyEmitted = true;
            return;
        }
        emitRows(output, AkeneoResource.FAMILY_VARIANTS, childPage, position.child, family.id);
        if (!closed && position.child.offset == childPage.rows.size()) {
            if (childPage.more) {
                position.child.advance(AkeneoResource.FAMILY_VARIANTS, null, config.maxPages);
            } else {
                position.parent++;
                position.familyEmitted = false;
                position.child = new AkeneoSplit.Position();
            }
            childPage = null;
        }
    }

    private void emitRows(
            Collector<SeaTunnelRow> output,
            AkeneoResource resource,
            AkeneoPage page,
            AkeneoSplit.Position state,
            String family) {
        int stop = Math.min(page.rows.size(), state.offset + EMIT_PER_POLL);
        while (!closed && state.offset < stop) {
            emit(output, resource, page.rows.get(state.offset), family);
            state.prefix = page.digest;
            state.offset++;
        }
    }

    private static void emit(
            Collector<SeaTunnelRow> output,
            AkeneoResource resource,
            AkeneoPage.Row row,
            String family) {
        SeaTunnelRow value =
                new SeaTunnelRow(new Object[] {resource.optionName(), row.id, family, row.record});
        value.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(value);
    }

    @Override
    public List<AkeneoSplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return position == null
                    ? Collections.emptyList()
                    : Collections.singletonList(position.copy());
        }
    }

    @Override
    public void addSplits(List<AkeneoSplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (position != null || splits.size() != 1 || context.getIndexOfSubtask() != 0) {
                throw new IllegalStateException("Invalid Akeneo reader assignment");
            }
            AkeneoSplit incoming = splits.get(0);
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
