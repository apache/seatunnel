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

package org.apache.seatunnel.connectors.seatunnel.activecampaign;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.math.BigInteger;
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

final class ActiveCampaignReader implements SourceReader<SeaTunnelRow, ActiveCampaignSplit> {
    private static final int ROWS_PER_POLL = 16;
    private final ActiveCampaignSourceOptions.Settings settings;
    private final Context context;
    private final Object stateLock = new Object();
    private final Object lifecycle = new Object();
    private volatile boolean closed;
    private ActiveCampaignHttp client;
    private ExecutorService worker;
    private ActiveCampaignSplit split;
    private boolean noMoreSplits;
    private boolean signaled;
    private Future<Page> pending;
    private long deadline;
    private Page page;

    ActiveCampaignReader(ActiveCampaignSourceOptions.Settings settings, Context context) {
        this.settings = settings;
        this.context = context;
    }

    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && context.getIndexOfSubtask() == 0 && worker == null) {
                client = new ActiveCampaignHttp(settings);
                worker =
                        Executors.newSingleThreadExecutor(
                                task -> ActiveCampaignHttp.daemon(task, "activecampaign-reader"));
            }
        }
    }

    /** Uses at most one in-flight page and leaves the engine thread available for barriers. */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        String resource;
        synchronized (stateLock) {
            if (closed || signaled) {
                return;
            }
            if ((split == null && noMoreSplits)
                    || (split != null && split.resource == settings.resources.size())) {
                signaled = true;
                context.signalNoMoreElement();
                return;
            }
            if (split == null) {
                return;
            }
            resource = settings.resources.get(split.resource);
        }
        if (page == null && !receive(resource)) {
            return;
        }
        synchronized (output.getCheckpointLock()) {
            synchronized (stateLock) {
                if (!closed) {
                    emit(output);
                }
            }
        }
    }

    private boolean receive(String resource) throws Exception {
        if (pending == null) {
            ActiveCampaignSplit position;
            synchronized (stateLock) {
                position = split.copy();
            }
            long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.deadline);
            synchronized (lifecycle) {
                if (closed) {
                    return false;
                }
                deadline = expires;
                pending = worker.submit(() -> prepare(resource, position, expires));
            }
        }
        try {
            page = pending.get(5, TimeUnit.MILLISECONDS);
            pending = null;
        } catch (TimeoutException timeout) {
            if (deadline - System.nanoTime() <= 0 && !closed) {
                cancel();
                throw ActiveCampaignHttp.failure(resource, "page deadline exceeded");
            }
            return false;
        } catch (CancellationException ex) {
            if (closed) {
                return false;
            }
            throw ActiveCampaignHttp.failure(resource, "request canceled");
        } catch (ExecutionException ex) {
            if (closed) {
                return false;
            }
            if (ex.getCause() instanceof ActiveCampaignHttp.Failure) {
                throw (ActiveCampaignHttp.Failure) ex.getCause();
            }
            throw ActiveCampaignHttp.failure(resource, "page could not be read");
        }
        if (deadline - System.nanoTime() <= 0) {
            throw ActiveCampaignHttp.failure(resource, "page deadline exceeded");
        }
        return !closed;
    }

    private Page prepare(String resource, ActiveCampaignSplit position, long expires)
            throws InterruptedException {
        JsonNode response = client.fetch(resource, position.offset, expires);
        JsonNode records = response.get(resource);
        if (records == null || !records.isArray() || records.size() > settings.pageSize) {
            throw ActiveCampaignHttp.failure(resource, "missing or oversized resource array");
        }
        int next = nextOffset(resource, position, records.size(), response.get("meta"));
        Set<String> ids = new TreeSet<>();
        List<SeaTunnelRow> rows = new ArrayList<>(records.size());
        StringBuilder orderedRecords = new StringBuilder();
        for (JsonNode item : records) {
            client.check(resource, expires);
            JsonNode idNode = item.get("id");
            if (!item.isObject()
                    || idNode == null
                    || !(idNode.isTextual() || idNode.isIntegralNumber())) {
                throw ActiveCampaignHttp.failure(resource, "resource record has no valid ID");
            }
            String id = idNode.asText();
            if (id.isBlank() || id.length() > 256 || !ids.add(id)) {
                throw ActiveCampaignHttp.failure(
                        resource, "resource record has an invalid or repeated ID");
            }
            String value = item.toString();
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {resource, id, value});
            row.setTableId(TablePath.DEFAULT.getFullName());
            rows.add(row);
            orderedRecords.append(value).append('\n');
        }
        byte[] digest = ActiveCampaignSourceOptions.digest(orderedRecords.toString());
        if (position.emitted != 0
                && (rows.size() <= position.emitted
                        || !MessageDigest.isEqual(digest, position.pageDigest))) {
            throw ActiveCampaignHttp.failure(resource, "checkpointed page changed during recovery");
        }
        String fingerprint = null;
        if (!ids.isEmpty()) {
            StringBuilder idSet = new StringBuilder();
            for (String id : ids) {
                idSet.append(id.length()).append(':').append(id);
            }
            fingerprint =
                    Base64.getEncoder()
                            .encodeToString(ActiveCampaignSourceOptions.digest(idSet.toString()));
            if (position.history.contains(fingerprint)) {
                throw ActiveCampaignHttp.failure(resource, "pagination repeated a previous page");
            }
        }
        client.check(resource, expires);
        return new Page(rows, digest, fingerprint, next);
    }

    private int nextOffset(
            String resource, ActiveCampaignSplit position, int count, JsonNode meta) {
        BigInteger total = total(resource, meta);
        if (count > 0
                && total != null
                && total.compareTo(BigInteger.valueOf((long) position.offset + count)) < 0) {
            throw ActiveCampaignHttp.failure(
                    resource, "meta.total is inconsistent with the current page");
        }
        int next = position.offset + settings.pageSize;
        boolean more =
                total == null
                        ? count == settings.pageSize
                        : total.compareTo(BigInteger.valueOf(next)) > 0;
        if (!more) {
            return -1;
        }
        if (next / settings.pageSize >= settings.maxPages) {
            throw ActiveCampaignHttp.failure(resource, "max_pages exceeded");
        }
        if (count == 0 && position.emptyPages == ActiveCampaignSplit.EMPTY_LIMIT) {
            throw ActiveCampaignHttp.failure(
                    resource, "too many consecutive empty continuation pages");
        }
        return next;
    }

    private static BigInteger total(String resource, JsonNode meta) {
        if (meta == null || (meta.isObject() && !meta.has("total"))) {
            return null;
        }
        JsonNode number = meta.get("total");
        if (number != null && (number.isIntegralNumber() || number.isTextual())) {
            String value = number.asText();
            if (value.length() <= 40 && value.matches("[0-9]+")) {
                return new BigInteger(value);
            }
        }
        throw ActiveCampaignHttp.failure(resource, "invalid meta.total");
    }

    /** The row and the reader position advance under the same checkpoint lock. */
    private void emit(Collector<SeaTunnelRow> output) {
        int end = Math.min(page.rows.size(), split.emitted + ROWS_PER_POLL);
        while (!closed && split.emitted < end) {
            output.collect(page.rows.get(split.emitted));
            split.pageDigest = page.digest;
            split.emitted++;
        }
        if (!closed && split.emitted == page.rows.size()) {
            split.completePage(page.nextOffset, page.fingerprint, page.rows.isEmpty());
            page = null;
        }
    }

    @Override
    public List<ActiveCampaignSplit> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return split == null
                    ? Collections.emptyList()
                    : Collections.singletonList(split.copy());
        }
    }

    @Override
    public void addSplits(List<ActiveCampaignSplit> splits) {
        if (splits.size() != 1 || context.getIndexOfSubtask() != 0) {
            throw new IllegalStateException("Invalid ActiveCampaign reader assignment");
        }
        ActiveCampaignSplit incoming = splits.get(0);
        settings.verify(incoming);
        synchronized (stateLock) {
            if (split != null) {
                throw new IllegalStateException("Duplicate ActiveCampaign reader assignment");
            }
            split = incoming.copy();
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (stateLock) {
            noMoreSplits = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void cancel() {
        synchronized (lifecycle) {
            closed = true;
            if (client != null) {
                client.close();
            }
            if (worker != null) {
                worker.shutdownNow();
            }
        }
    }

    @Override
    public void close() {
        cancel();
    }

    private static final class Page {
        private final List<SeaTunnelRow> rows;
        private final byte[] digest;
        private final String fingerprint;
        private final int nextOffset;

        private Page(List<SeaTunnelRow> rows, byte[] digest, String fingerprint, int nextOffset) {
            this.rows = rows;
            this.digest = digest;
            this.fingerprint = fingerprint;
            this.nextOffset = nextOffset;
        }
    }
}
