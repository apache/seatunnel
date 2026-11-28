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

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.HashSet;
import java.util.Set;

/**
 * Stores the resource, page and emitted row position for a Cal.com scan.
 *
 * <p>Cursor-mode booking pages retain only visited fingerprints plus the current continuation.
 * Large response objects and the API key stay outside the checkpoint.
 */
final class CalComSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String scope;
    int resourceIndex;
    int pages;
    String cursor;
    int row;
    String digest;
    final Set<String> cursorHistory = new HashSet<>();

    CalComSplit(String scope) {
        this.scope = scope;
    }

    @Override
    public String splitId() {
        return "cal-com-bounded-scan";
    }

    boolean finished(CalComConfig config) {
        return resourceIndex == config.resources.size();
    }

    /**
     * Creates an independent snapshot of the accepted page and row.
     *
     * <p>Checkpoints never share mutable continuation history with the active reader.
     */
    CalComSplit copy() {
        CalComSplit copy = new CalComSplit(scope);
        copy.resourceIndex = resourceIndex;
        copy.pages = pages;
        copy.cursor = cursor;
        copy.row = row;
        copy.digest = digest;
        copy.cursorHistory.addAll(cursorHistory);
        return copy;
    }

    void validate(CalComConfig config) {
        if (!config.scope.equals(scope)) {
            throw failure("credential, selection or API version changed");
        }
        if (resourceIndex < 0 || resourceIndex > config.resources.size()) {
            throw failure("invalid resource position");
        }
        if (finished(config)) {
            if (pages != 0
                    || row != 0
                    || digest != null
                    || cursor != null
                    || !cursorHistory.isEmpty()) {
                throw failure("invalid terminal position");
            }
            return;
        }
        CalComResource resource = config.resources.get(resourceIndex);
        boolean bookings = resource == CalComResource.BOOKINGS;
        if (pages < 0
                || pages > (bookings ? config.maxPages : 1)
                || row < 0
                || row > config.expectedMax(resource)
                || digest == null && (row != 0 || !bookings && pages != 0)
                || digest != null && (digest.length() != 64 || pages == 0)) {
            throw failure("invalid page or row position");
        }
        if (bookings && config.cursorBookings()) {
            if (cursor != null
                            && (!CalComApi.validOpaque(cursor)
                                    || !cursorHistory.contains(CalComConfig.hash(cursor)))
                    || cursorHistory.size() != pageIndex()
                    || (cursor == null) != cursorHistory.isEmpty()) {
                throw failure("invalid booking cursor position");
            }
        } else if (cursor != null || !cursorHistory.isEmpty()) {
            throw failure("unexpected booking cursor");
        }
    }

    int pageIndex() {
        return pages - (digest == null ? 0 : 1);
    }

    void nextPage(String next) {
        cursor = next;
        if (next != null) {
            cursorHistory.add(CalComConfig.hash(next));
        }
        row = 0;
        digest = null;
    }

    void nextResource() {
        ++resourceIndex;
        cursor = null;
        cursorHistory.clear();
        row = 0;
        digest = null;
        pages = 0;
    }

    private static CalComResource.Failure failure(String reason) {
        return new CalComResource.Failure("checkpoint", reason);
    }
}
