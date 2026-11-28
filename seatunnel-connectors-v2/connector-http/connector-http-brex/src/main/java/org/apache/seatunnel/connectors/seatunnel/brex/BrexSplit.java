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
package org.apache.seatunnel.connectors.seatunnel.brex;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Stores a bounded Brex traversal, opaque page cursor and exact emitted row position.
 *
 * <p>The full response must match the saved digest before any suffix can be emitted on restore.
 * Authentication material and records never enter the checkpoint.
 */
final class BrexSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String scope;
    final Instant end;

    // Cursor and its history belong exclusively to the current collection.
    int resourceIndex;
    int pages;
    String cursor;
    int row;
    String digest;
    final Set<String> cursorHistory = new HashSet<>();

    BrexSplit(String scope, Instant end) {
        this.scope = scope;
        this.end = end;
    }

    @Override
    public String splitId() {
        return "brex-bounded-scan";
    }

    boolean finished(BrexConfig config) {
        return resourceIndex == config.resources.size();
    }

    /**
     * Makes a detached snapshot for the coordinator or reader lifecycle.
     *
     * <p>Neither the cursor history nor the position is shared with live state.
     */
    BrexSplit copy() {
        BrexSplit copy = new BrexSplit(scope, end);
        copy.resourceIndex = resourceIndex;
        copy.pages = pages;
        copy.cursor = cursor;
        copy.row = row;
        copy.digest = digest;
        copy.cursorHistory.addAll(cursorHistory);
        return copy;
    }

    void validate(BrexConfig config) {
        if (!config.scope.equals(scope)
                || end == null
                || config.end != null && !config.end.equals(end)) {
            throw new BrexResource.Failure(
                    "checkpoint", "credential, resource or scan scope changed");
        }
        if (resourceIndex < 0
                || resourceIndex > config.resources.size()
                || pages < 0
                || pages > config.maxPages
                || row < 0
                || row > config.pageSize
                || digest == null && row != 0
                || digest != null && (digest.length() != 64 || pages == 0)
                || cursor != null
                        && (!BrexApi.validCursor(cursor)
                                || !cursorHistory.contains(BrexConfig.hash(cursor)))
                || cursorHistory.size() != pages - (digest == null ? 0 : 1)
                || (cursor == null) != cursorHistory.isEmpty()
                || finished(config) && (pages != 0 || cursor != null)) {
            throw new BrexResource.Failure("checkpoint", "invalid page state");
        }
    }

    void nextPage(String next) {
        cursor = next;
        cursorHistory.add(BrexConfig.hash(next));
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
}
