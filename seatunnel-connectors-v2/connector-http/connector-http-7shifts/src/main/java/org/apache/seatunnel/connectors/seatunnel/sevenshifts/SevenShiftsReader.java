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

package org.apache.seatunnel.connectors.seatunnel.sevenshifts;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsHttp.failure;

/** Walks company and user scopes, committing each complete page to the collector and split. */
final class SevenShiftsReader implements SourceReader<SeaTunnelRow, SevenShiftsSplit> {
    private static final int MAX_RECORDS_PER_PAGE = 10000;
    private final SevenShiftsConfig config;
    private final Context context;
    private final String tableId;
    private final SevenShiftsHttp http;
    private final Object stateLock = new Object();

    private SevenShiftsSplit split;
    private PageRequest request;
    private volatile boolean cancelled;
    private boolean signaledDone;

    SevenShiftsReader(SevenShiftsConfig config, Context context, String tableId) {
        this.config = config;
        this.context = context;
        this.tableId = tableId;
        http = new SevenShiftsHttp(config);
    }

    @Override
    public void open() {}

    /** Waits briefly when the single asynchronous operation has no result yet. */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (cancelled || signaledDone) {
            return;
        }
        boolean waiting;
        boolean finished;
        synchronized (stateLock) {
            waiting = split == null;
            finished = !waiting && split.step == SevenShiftsSplit.Step.DONE;
            if (!waiting && !finished && request == null) {
                request = request(split);
                http.start(request.resource, request.route);
                return;
            }
        }
        if (finished) {
            signaledDone = true;
            context.signalNoMoreElement();
            return;
        }
        if (waiting) {
            Thread.sleep(10);
            return;
        }
        JsonNode result = http.poll();
        if (result == null) {
            if (!cancelled) {
                Thread.sleep(10);
            }
            return;
        }

        Page page = decode(request, result);
        synchronized (output.getCheckpointLock()) {
            synchronized (stateLock) {
                if (cancelled) {
                    return;
                }
                String digest = split.validateNext(request.resource, page.next);
                if (request.discovery) {
                    split.addParents(page.parents, config.resource);
                } else {
                    page.rows.forEach(output::collect);
                }
                split.advance(config.resource, page.next, digest);
                request = null;
            }
        }
    }

    private PageRequest request(SevenShiftsSplit position) {
        SevenShiftsResource resource = config.resource;
        Long companyId = position.companyId();
        Long userId = position.userId();
        boolean discovery = position.step != SevenShiftsSplit.Step.RECORDS;
        if (position.step == SevenShiftsSplit.Step.COMPANIES) {
            resource = SevenShiftsResource.COMPANIES;
            companyId = null;
        } else if (position.step == SevenShiftsSplit.Step.USERS) {
            resource = SevenShiftsResource.USERS;
            userId = null;
        }
        List<String> query = new ArrayList<>(3);
        if (resource.scope == SevenShiftsResource.Scope.COMPANY) {
            query.add("limit=" + config.pageSize);
        }
        String since = discovery ? null : config.providerCutoff();
        if (since != null) {
            query.add("modified_since=" + encode(since));
        }
        if (position.cursor != null) {
            query.add("cursor=" + encode(position.cursor));
        }
        String route = resource.route(companyId, userId);
        if (!query.isEmpty()) {
            route += "?" + String.join("&", query);
        }
        return new PageRequest(resource, route, discovery, companyId, userId);
    }

    private Page decode(PageRequest current, JsonNode result)
            throws SevenShiftsHttp.RequestFailure {
        JsonNode error = result.path("error");
        if (!error.isMissingNode() && !error.isNull()) {
            throw failure(current.resource, "HTTP 200 returned an error envelope");
        }
        JsonNode data = result.path("data");
        Page page = new Page(nextCursor(current.resource, result));
        if (current.resource.shape == SevenShiftsResource.Shape.OBJECT) {
            if (!data.isObject()) {
                throw failure(current.resource, "data must be an object");
            }
            requireBuckets(current.resource, data);
            decodeRecord(current, data, page);
        } else {
            if (!data.isArray() || data.size() > MAX_RECORDS_PER_PAGE) {
                throw failure(current.resource, "data must be an array of at most 10000 objects");
            }
            for (JsonNode record : data) {
                decodeRecord(current, record, page);
            }
        }
        return page;
    }

    private void decodeRecord(PageRequest current, JsonNode record, Page page)
            throws SevenShiftsHttp.RequestFailure {
        SevenShiftsResource resource = current.resource;
        if (!record.isObject()) {
            throw failure(resource, "data contains a value that is not an object");
        }
        Long companyId = current.companyId;
        Long userId = current.userId;
        if (resource == SevenShiftsResource.COMPANIES) {
            companyId = id(resource, record.path("id"), false);
        } else {
            validateParent(resource, companyId, record.path("company_id"));
        }
        if (resource == SevenShiftsResource.USERS) {
            userId = id(resource, record.path("id"), false);
        } else if (userId != null) {
            validateParent(resource, userId, record.path("user_id"));
        } else if (!absent(record.path("user_id"))) {
            userId = id(resource, record.path("user_id"), true);
        }
        if (current.discovery) {
            page.parents.add(resource == SevenShiftsResource.COMPANIES ? companyId : userId);
        } else if (modifiedWithinRange(resource, record.path("modified"))) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {resource.apiName, companyId, userId, record.toString()});
            row.setTableId(tableId);
            page.rows.add(row);
        }
    }

    private boolean modifiedWithinRange(SevenShiftsResource resource, JsonNode modified)
            throws SevenShiftsHttp.RequestFailure {
        if (config.startDate == null || absent(modified)) {
            return true;
        }
        try {
            if (modified.isTextual()) {
                return !SevenShiftsConfig.parseTimestamp(modified.textValue())
                        .isBefore(config.startDate);
            }
        } catch (DateTimeParseException e) {
            throw failure(resource, "modified is not an offset timestamp");
        }
        throw failure(resource, "modified is not an offset timestamp");
    }

    private static String nextCursor(SevenShiftsResource resource, JsonNode result)
            throws SevenShiftsHttp.RequestFailure {
        JsonNode meta = result.path("meta");
        if (absent(meta)) {
            return null;
        }
        if (!meta.isObject()) {
            throw failure(resource, "meta must be an object");
        }
        JsonNode cursor = meta.path("cursor");
        if (absent(cursor)) {
            return null;
        }
        if (!cursor.isObject()) {
            throw failure(resource, "meta.cursor must be an object");
        }
        JsonNode next = cursor.path("next");
        if (absent(next)) {
            return null;
        }
        if (resource.scope == SevenShiftsResource.Scope.USER) {
            throw failure(resource, "unexpected pagination on a user endpoint");
        }
        if (!next.isTextual() || next.textValue().isEmpty() || next.textValue().length() > 2048) {
            throw failure(resource, "invalid next cursor");
        }
        return next.textValue();
    }

    private static void requireBuckets(SevenShiftsResource resource, JsonNode data)
            throws SevenShiftsHttp.RequestFailure {
        String[] names =
                resource == SevenShiftsResource.WAGES
                        ? new String[] {"current_wages", "upcoming_wages"}
                        : new String[] {"locations", "departments", "roles"};
        for (String name : names) {
            if (!data.path(name).isArray()) {
                throw failure(resource, "data is missing a required array");
            }
        }
    }

    private static void validateParent(SevenShiftsResource resource, Long expected, JsonNode value)
            throws SevenShiftsHttp.RequestFailure {
        if (!absent(value) && !Long.valueOf(id(resource, value, false)).equals(expected)) {
            throw failure(resource, "response parent ID does not match the request");
        }
    }

    private static long id(SevenShiftsResource resource, JsonNode value, boolean allowZero)
            throws SevenShiftsHttp.RequestFailure {
        if (!value.isIntegralNumber()
                || !value.canConvertToLong()
                || value.longValue() < (allowZero ? 0 : 1)) {
            throw failure(resource, "response ID is not a valid signed 64-bit integer");
        }
        return value.longValue();
    }

    private static boolean absent(JsonNode value) {
        return value.isMissingNode() || value.isNull();
    }

    private static String encode(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 is unavailable", e);
        }
    }

    /** An empty reader before assignment is distinct from an assigned terminal split. */
    @Override
    public List<SevenShiftsSplit> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return split == null
                    ? Collections.emptyList()
                    : Collections.singletonList(split.copy());
        }
    }

    @Override
    public void addSplits(List<SevenShiftsSplit> splits) {
        synchronized (stateLock) {
            for (SevenShiftsSplit incoming : splits) {
                if (split != null || !config.checkpointKey().equals(incoming.checkpointKey)) {
                    throw SevenShiftsConfig.invalid("received an unexpected scan split");
                }
                split = incoming;
            }
        }
    }

    @Override
    public void handleNoMoreSplits() {
        // The assigned split itself identifies a completed scan, including on restore.
    }

    @Override
    public void cancel() {
        cancelled = true;
        http.close();
    }

    @Override
    public void close() {
        cancel();
    }

    private static final class PageRequest {
        private final SevenShiftsResource resource;
        private final String route;
        private final boolean discovery;
        private final Long companyId;
        private final Long userId;

        private PageRequest(
                SevenShiftsResource resource,
                String route,
                boolean discovery,
                Long companyId,
                Long userId) {
            this.resource = resource;
            this.route = route;
            this.discovery = discovery;
            this.companyId = companyId;
            this.userId = userId;
        }
    }

    private static final class Page {
        private final String next;
        private final Set<Long> parents = new LinkedHashSet<>();
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        private Page(String next) {
            this.next = next;
        }
    }
}
