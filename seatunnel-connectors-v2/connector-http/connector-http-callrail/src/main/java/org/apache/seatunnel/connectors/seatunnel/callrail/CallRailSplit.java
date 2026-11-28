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
package org.apache.seatunnel.connectors.seatunnel.callrail;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Frozen account scan position, date slice and same-scope pagination identity.
 *
 * <p>Only the accepted continuation and compact history belong in state. Provider records and
 * credentials remain outside the checkpoint.
 */
final class CallRailSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private static final int SLICE_DAYS = 100;

    final String scope;
    final Instant end;
    final int resourceIndex;
    final LocalDate windowStart;
    final int page;
    final int requests;
    final int observedSize;
    final String nextUrl;
    final List<String> previousIds;
    private final List<String> seenPages;
    final String fingerprint;
    final int published;

    CallRailSplit(CallRailConfig config) {
        this(
                config.scope,
                config.scanEnd(),
                0,
                date(config.start),
                1,
                0,
                0,
                null,
                List.of(),
                List.of(),
                null,
                0);
    }

    private CallRailSplit(
            String scope,
            Instant end,
            int resourceIndex,
            LocalDate windowStart,
            int page,
            int requests,
            int observedSize,
            String nextUrl,
            List<String> previousIds,
            List<String> seenPages,
            String fingerprint,
            int published) {
        this.scope = scope;
        this.end = end;
        this.resourceIndex = resourceIndex;
        this.windowStart = windowStart;
        this.page = page;
        this.requests = requests;
        this.observedSize = observedSize;
        this.nextUrl = nextUrl;
        this.previousIds = List.copyOf(previousIds);
        this.seenPages = List.copyOf(seenPages);
        this.fingerprint = fingerprint;
        this.published = published;
    }

    Instant windowEnd() {
        Instant next = windowStart.plusDays(SLICE_DAYS).atStartOfDay(ZoneOffset.UTC).toInstant();
        return end.isBefore(next) ? end : next;
    }

    String requestEndDate() {
        return date(windowEnd().minusNanos(1)).toString();
    }

    static LocalDate date(Instant time) {
        return time.atOffset(ZoneOffset.UTC).toLocalDate();
    }

    CallRailSplit partial(String fingerprint, int published) {
        return new CallRailSplit(
                scope,
                end,
                resourceIndex,
                windowStart,
                page,
                requests,
                observedSize,
                nextUrl,
                previousIds,
                seenPages,
                fingerprint,
                published);
    }

    CallRailSplit advance(
            CallRailConfig config,
            CallRailResource resource,
            boolean more,
            int perPage,
            String continuation,
            List<String> ids)
            throws CallRailReadException {
        List<String> updated = history(resource, ids);
        if (more) {
            return new CallRailSplit(
                    scope,
                    end,
                    resourceIndex,
                    windowStart,
                    page + 1,
                    requests + 1,
                    perPage,
                    continuation,
                    ids,
                    updated,
                    null,
                    0);
        }
        Instant sliceEnd = resource.dateFiltered ? windowEnd() : end;
        boolean nextWindow = sliceEnd.isBefore(end);
        return new CallRailSplit(
                scope,
                end,
                nextWindow ? resourceIndex : resourceIndex + 1,
                nextWindow ? date(sliceEnd) : date(config.start),
                1,
                requests + 1,
                0,
                null,
                List.of(),
                List.of(),
                null,
                0);
    }

    private List<String> history(CallRailResource resource, List<String> ids)
            throws CallRailReadException {
        if (ids.isEmpty()) {
            return seenPages;
        }
        List<String> sorted = new ArrayList<>(ids);
        Collections.sort(sorted);
        String identity = CallRailConfig.digest(String.join("", sorted));
        int position = Collections.binarySearch(seenPages, identity);
        if (position >= 0) {
            throw resource.failure(page, "provider repeated an earlier page");
        }
        List<String> updated = new ArrayList<>(seenPages);
        updated.add(-position - 1, identity);
        return updated;
    }

    @Override
    public String splitId() {
        return "callrail-account";
    }
}
