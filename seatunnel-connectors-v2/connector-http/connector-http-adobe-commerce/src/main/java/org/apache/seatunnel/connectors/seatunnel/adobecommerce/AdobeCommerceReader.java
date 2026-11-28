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
package org.apache.seatunnel.connectors.seatunnel.adobecommerce;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class AdobeCommerceReader implements SourceReader<SeaTunnelRow, AdobeCommerceSplit> {
    private static final int ROWS_PER_POLL = 16;
    private final Context context;
    private final AdobeCommerceSettings settings;
    private final Object stateLock = new Object();
    private final Object requestLock = new Object();
    private volatile boolean closed;
    private boolean inputEnded;
    private boolean ended;
    private AdobeCommerceSplit position;

    private AdobeCommerceTransport transport;
    private ExecutorService executor;
    private volatile Fetch fetch;
    private Page ready;

    AdobeCommerceReader(AdobeCommerceSettings settings, Context context) {
        this.settings = settings;
        this.context = context;
    }

    @Override
    public void open() {
        synchronized (requestLock) {
            if (!closed && executor == null && context.getIndexOfSubtask() == 0) {
                transport = new AdobeCommerceTransport(settings);
                executor =
                        Executors.newSingleThreadExecutor(
                                task ->
                                        AdobeCommerceTransport.thread(
                                                task, "adobe-commerce-reader"));
            }
        }
    }

    /** Fetch and validate one page off the task thread while checkpoints can still complete. */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        AdobeCommerceResource resource;
        synchronized (stateLock) {
            if (closed || ended) {
                return;
            }
            if (position == null && inputEnded
                    || position != null && position.resource == settings.resources.size()) {
                ended = true;
                context.signalNoMoreElement();
                return;
            }
            if (position == null) {
                return;
            }
            resource = settings.resources.get(position.resource);
        }
        if (ready == null && !pollPage(resource)) {
            return;
        }
        synchronized (output.getCheckpointLock()) {
            synchronized (stateLock) {
                collect(output);
            }
        }
    }

    private boolean pollPage(AdobeCommerceResource resource) throws Exception {
        Fetch current = fetch;
        if (current == null) {
            AdobeCommerceSplit request;
            synchronized (stateLock) {
                request = position.copy();
            }
            synchronized (requestLock) {
                if (closed) {
                    return false;
                }
                long deadline =
                        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.pageDeadline);
                current =
                        new Fetch(
                                deadline, executor.submit(() -> read(resource, request, deadline)));
                fetch = current;
            }
        }
        try {
            ready = current.result.get(5, TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeout) {
            if (!closed && current.deadline - System.nanoTime() <= 0) {
                cancel();
                throw resource.error("page deadline exceeded");
            }
            return false;
        } catch (ExecutionException ex) {
            if (closed) {
                return false;
            }
            if (ex.getCause() instanceof AdobeCommerceResource.Failure) {
                throw (AdobeCommerceResource.Failure) ex.getCause();
            }
            throw resource.error("request could not be completed");
        } catch (CancellationException ex) {
            if (closed) {
                return false;
            }
            throw resource.error("request was canceled");
        }
        fetch = null;
        return !closed;
    }

    private Page read(AdobeCommerceResource resource, AdobeCommerceSplit state, long deadline)
            throws InterruptedException {
        JsonNode root = transport.fetch(resource, resource.target(settings, state), deadline);
        List<JsonNode> records = resource.records(root, settings.limit);
        boolean hasNext = resource.hasNext(root, state.page, records.size(), settings);
        List<SeaTunnelRow> rows = new ArrayList<>(records.size());
        Set<String> ids = new TreeSet<>();
        MessageDigest content = AdobeCommerceSettings.digest();
        for (JsonNode item : records) {
            transport.needTime(resource, deadline);
            if (!item.isObject()) {
                throw resource.error("invalid resource row");
            }
            resource.verifyWindow(item, settings.start, state.end);
            String id = resource.id(item);
            if (!ids.add(id)) {
                throw resource.error("repeated resource identity in a page");
            }
            String encoded = item.toString();
            content.update(encoded.getBytes(StandardCharsets.UTF_8));
            content.update((byte) '\n');
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {resource.label(), id, encoded});
            row.setTableId(TablePath.DEFAULT.getFullName());
            rows.add(row);
        }

        byte[] pageDigest = content.digest();
        if (state.emitted != 0
                && (rows.size() <= state.emitted
                        || !MessageDigest.isEqual(pageDigest, state.digest))) {
            throw resource.error("checkpointed page changed during recovery");
        }
        MessageDigest stableIds = AdobeCommerceSettings.digest();
        for (String id : ids) {
            stableIds.update(Integer.toString(id.length()).getBytes(StandardCharsets.US_ASCII));
            stableIds.update((byte) ':');
            stableIds.update(id.getBytes(StandardCharsets.UTF_8));
        }
        String fingerprint = Base64.getEncoder().encodeToString(stableIds.digest());
        if (!ids.isEmpty() && state.history.contains(fingerprint)) {
            throw resource.error("pagination repeated a previous page");
        }
        transport.needTime(resource, deadline);
        return new Page(rows, pageDigest, fingerprint, hasNext);
    }

    /** Commit the row index with the collector's checkpoint lock, including the page transition. */
    private void collect(Collector<SeaTunnelRow> output) {
        int end = Math.min(position.emitted + ROWS_PER_POLL, ready.rows.size());
        while (!closed && position.emitted < end) {
            output.collect(ready.rows.get(position.emitted));
            position.digest = ready.digest;
            position.emitted++;
        }
        if (!closed && position.emitted == ready.rows.size()) {
            position.finish(ready.more, ready.fingerprint);
            ready = null;
        }
    }

    @Override
    public List<AdobeCommerceSplit> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return position == null
                    ? Collections.emptyList()
                    : Collections.singletonList(position.copy());
        }
    }

    @Override
    public void addSplits(List<AdobeCommerceSplit> splits) {
        synchronized (stateLock) {
            if (splits.size() != 1 || context.getIndexOfSubtask() != 0 || position != null) {
                throw new IllegalStateException("Invalid AdobeCommerce reader assignment");
            }
            AdobeCommerceSplit assigned = splits.get(0);
            settings.verify(assigned);
            position = assigned.copy();
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (stateLock) {
            inputEnded = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void cancel() {
        synchronized (requestLock) {
            closed = true;
            if (transport != null) {
                transport.close();
            }
            Fetch pending = fetch;
            if (pending != null) {
                pending.result.cancel(true);
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

    private static final class Fetch {
        private final long deadline;
        private final Future<Page> result;

        private Fetch(long deadline, Future<Page> result) {
            this.deadline = deadline;
            this.result = result;
        }
    }

    private static final class Page {
        private final List<SeaTunnelRow> rows;
        private final byte[] digest;
        private final String fingerprint;
        private final boolean more;

        private Page(List<SeaTunnelRow> rows, byte[] digest, String fingerprint, boolean more) {
            this.rows = rows;
            this.digest = digest;
            this.fingerprint = fingerprint;
            this.more = more;
        }
    }
}
