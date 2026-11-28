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
package org.apache.seatunnel.connectors.seatunnel.braintree;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Resumable position for a Braintree merchant and frozen scan window.
 *
 * <p>Search IDs, native batch boundaries and consumed response fingerprints survive recovery. No
 * connection credentials or provider records are serialized into this split.
 */
final class BraintreeSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final Instant start;
    final Instant end;

    // Dates are represented as whole UTC minutes or epoch days, according to the resource.
    long nextWindow = Long.MIN_VALUE;
    Window window;
    final Deque<Window> subdivisions;
    int windows;
    int resourceIndex;
    int pages;

    // Paged resources use one-based page indices and validate provider metadata across pages.
    int page = 1;
    int total = -1;
    int size;
    final Set<String> pageDigests;

    // An ID search binds all subsequent requests to its ordered, capped result.
    List<String> ids;
    int batchSize;
    int idOffset;
    int row;
    String digest;

    BraintreeSplit(String scope, Instant start, Instant end) {
        this.scope = scope;
        this.start = start;
        this.end = end;
        subdivisions = new ArrayDeque<>();
        pageDigests = new HashSet<>();
    }

    private BraintreeSplit(BraintreeSplit prior) {
        scope = prior.scope;
        start = prior.start;
        end = prior.end;
        resourceIndex = prior.resourceIndex;
        nextWindow = prior.nextWindow;
        window = prior.window;
        subdivisions = new ArrayDeque<>(prior.subdivisions);
        windows = prior.windows;
        pages = prior.pages;
        page = prior.page;
        total = prior.total;
        size = prior.size;
        pageDigests = new HashSet<>(prior.pageDigests);
        ids = prior.ids;
        batchSize = prior.batchSize;
        idOffset = prior.idOffset;
        row = prior.row;
        digest = prior.digest;
    }

    @Override
    public String splitId() {
        return "braintree-merchant-scan";
    }

    boolean finished(BraintreeConfig config) {
        return resourceIndex == config.resources.size();
    }

    /**
     * Detaches mutable pagination and deferred-window state for coordinator serialization.
     *
     * <p>The ID batch plan and precise window items are themselves immutable.
     */
    BraintreeSplit copy() {
        return new BraintreeSplit(this);
    }

    /**
     * Rejects a split opened under a different environment, merchant, or scan configuration.
     *
     * <p>New credentials may be supplied for the same merchant path.
     */
    void validate(BraintreeConfig config) {
        if (!config.scope.equals(scope)
                || !Objects.equals(start, config.start)
                || end == null
                || start != null && end.isBefore(start)
                || resourceIndex < 0
                || resourceIndex > config.resources.size()
                || page < 1
                || pages < 0
                || pages > config.maxPages
                || windows < 0
                || windows > config.maxWindows
                || subdivisions.size() > 64
                || total < -1
                || size < 0
                || size > 1000
                || (total >= 0) != (size > 0)
                || row < 0
                || row > 0 && digest == null
                || digest != null && !digest.matches("[0-9a-f]{64}")
                || ids == null && (batchSize != 0 || idOffset != 0)
                || ids != null
                        && (ids.isEmpty()
                                || ids.size() >= config.resultLimit
                                || batchSize < 1
                                || batchSize > config.batchLimit
                                || idOffset < 0
                                || idOffset >= ids.size()
                                || idOffset % batchSize != 0)) {
            throw new BraintreeFailure("checkpoint scope or response position is invalid");
        }
        if (!finished(config)) {
            validateResource(config.resources.get(resourceIndex));
        }
    }

    private void validateResource(BraintreeResource resource) {
        if (!resource.dated()) {
            if (ids != null || window != null || !subdivisions.isEmpty()) {
                throw new BraintreeFailure("checkpoint has searches for an undated resource");
            }
            return;
        }
        if (start == null
                || (ids != null || digest != null) && window == null
                || ids != null
                        && (resource.mode != BraintreeResource.Mode.IDS
                                || ids.size() >= resource.providerLimit)) {
            throw new BraintreeFailure("checkpoint search batch and window are inconsistent");
        }
        long lower = Window.unit(start, resource.daily());
        long upper = Window.unit(end, resource.daily());
        if (window != null && !window.within(lower, upper)
                || subdivisions.stream().anyMatch(pending -> !pending.within(lower, upper))) {
            throw new BraintreeFailure("checkpoint search window is outside its frozen dates");
        }
    }

    void validatePage(BraintreeResource resource, String receivedDigest, int rows) {
        if (row > rows || digest != null && !digest.equals(receivedDigest)) {
            throw resource.failure("checkpoint response changed before replay");
        }
    }

    void clearPosition() {
        row = 0;
        digest = null;
    }

    void clearWindow() {
        window = null;
        page = 1;
        total = -1;
        size = 0;
        pageDigests.clear();
        ids = null;
        batchSize = 0;
        idOffset = 0;
        clearPosition();
    }

    void nextResource() {
        clearWindow();
        ++resourceIndex;
        nextWindow = Long.MIN_VALUE;
        windows = 0;
        pages = 0;
        subdivisions.clear();
    }

    /**
     * Describes one disjoint v6 search span in whole minute or day units.
     *
     * <p>The provider includes the minute specified as its upper timestamp because it advances that
     * value before searching. Received-date spans are partitioned on complete days.
     */
    static final class Window implements Serializable {
        private static final long serialVersionUID = 1L;
        final long first;
        final long last;

        Window(long first, long last) {
            this.first = first;
            this.last = last;
        }

        static long unit(Instant instant, boolean daily) {
            return daily
                    ? instant.atOffset(ZoneOffset.UTC).toLocalDate().toEpochDay()
                    : Math.floorDiv(instant.getEpochSecond(), 60L);
        }

        Instant min(boolean daily) {
            return daily
                    ? LocalDate.ofEpochDay(first).atStartOfDay().toInstant(ZoneOffset.UTC)
                    : Instant.ofEpochSecond(first * 60L);
        }

        Instant max(boolean daily) {
            return daily
                    ? LocalDate.ofEpochDay(last).atTime(23, 59).toInstant(ZoneOffset.UTC)
                    : Instant.ofEpochSecond(last * 60L);
        }

        boolean within(long lower, long upper) {
            return lower <= first && first <= last && last <= upper;
        }

        long middle() {
            return first + (last - first) / 2;
        }
    }
}
