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
package org.apache.seatunnel.connectors.seatunnel.canny;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.connectors.seatunnel.canny.CannyResource.Paging;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Saves the active workspace page and, when present, its current board’s child collection.
 *
 * <p>Only page digests, identifiers and row positions are stored. Credentials and response payloads
 * remain outside the checkpoint.
 */
final class CannySplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String scope;
    final List<Frame> frames = new ArrayList<>(2);
    int rootIndex;
    int acceptedPages;

    CannySplit(CannyConfig config) {
        this(config.scope);
        frames.add(new Frame(config.roots.get(0), null));
    }

    private CannySplit(String scope) {
        this.scope = scope;
    }

    @Override
    public String splitId() {
        return "canny-workspace";
    }

    boolean finished() {
        return frames.isEmpty();
    }

    /**
     * Copies the complete traversal state for an independent checkpoint.
     *
     * <p>Parent and child progress can advance after the copy is taken.
     */
    CannySplit copy() {
        CannySplit saved = new CannySplit(scope);
        saved.rootIndex = rootIndex;
        saved.acceptedPages = acceptedPages;
        frames.forEach(frame -> saved.frames.add(frame.copy()));
        return saved;
    }

    void validate(CannyConfig config) {
        if (!config.scope.equals(scope)) {
            throw failure("credential or paging scope changed");
        }
        if (rootIndex < 0
                || rootIndex > config.roots.size()
                || frames.size() > 2
                || finished() != (rootIndex == config.roots.size())
                || acceptedPages < 0
                || acceptedPages > config.scanPages) {
            throw failure("invalid workspace traversal position");
        }
        if (finished()) {
            return;
        }
        Frame root = frames.get(0);
        root.validate(config);
        if (root.resource != config.roots.get(rootIndex) || root.boardId != null) {
            throw failure("invalid workspace resource");
        }
        if (frames.size() == 2) {
            Frame child = frames.get(1);
            child.validate(config);
            if (root.resource != CannyResource.BOARDS
                    || !root.emitted
                    || root.child == 0
                    || child.boardId == null
                    || child.resource != config.children.get(root.child - 1)) {
                throw failure("invalid board category order");
            }
        }
    }

    void nextRoot(CannyConfig config) {
        frames.clear();
        ++rootIndex;
        if (rootIndex < config.roots.size()) {
            frames.add(new Frame(config.roots.get(rootIndex), null));
        }
    }

    private static CannyResource.Failure failure(String message) {
        return new CannyResource.Failure("checkpoint", message);
    }

    /**
     * Tracks the current row and child boundary in one workspace or board response.
     *
     * <p>The reader validates the owning board using the restored parent page before child I/O.
     */
    static final class Frame implements Serializable {
        private static final long serialVersionUID = 1L;
        final CannyResource resource;
        final String boardId;
        int page = 1;
        int row;
        int child;
        boolean emitted;
        String digest;
        String cursor;

        // A bounded history of opaque cursor or nonempty offset-page hashes detects cycles.
        final Set<String> seen = new HashSet<>();

        Frame(CannyResource resource, String boardId) {
            this.resource = resource;
            this.boardId = boardId;
        }

        Frame copy() {
            Frame saved = new Frame(resource, boardId);
            saved.page = page;
            saved.row = row;
            saved.child = child;
            saved.emitted = emitted;
            saved.digest = digest;
            saved.cursor = cursor;
            saved.seen.addAll(seen);
            return saved;
        }

        void validate(CannyConfig config) {
            if (resource == null) {
                throw failure("missing resource");
            }
            int children = resource == CannyResource.BOARDS ? config.children.size() : 0;
            int rows = resource.pageLimit(config, page);
            if ((resource == CannyResource.CATEGORIES) != (boardId != null)
                    || boardId != null && !CannyApi.validOpaque(boardId)
                    || page < 1
                    || page > config.maxPages
                    || resource.paging == Paging.COMPLETE && page != 1
                    || resource == CannyResource.CHANGELOG_ENTRIES && page > 2
                    || !validHistory()
                    || row < 0
                    || row > rows
                    || child < 0
                    || child > children
                    || !emitted && child != 0
                    || digest == null && (row != 0 || emitted)
                    || digest != null && !validHash(digest)) {
                throw failure("invalid response row or page position");
            }
        }

        private boolean validHistory() {
            if (seen.size() > page - 1 || seen.stream().anyMatch(hash -> !validHash(hash))) {
                return false;
            }
            if (resource.paging != Paging.CURSOR || page == 1) {
                return cursor == null;
            }
            return cursor != null
                    && CannyApi.validOpaque(cursor)
                    && seen.size() == page - 1
                    && seen.contains(CannyConfig.hash(cursor));
        }

        private static boolean validHash(String value) {
            return value != null
                    && value.length() == 64
                    && value.chars().allMatch(c -> c >= '0' && c <= '9' || c >= 'a' && c <= 'f');
        }

        void advanceRow() {
            ++row;
            child = 0;
            emitted = false;
        }

        void nextPage(int next, String nextCursor, String history) {
            cursor = nextCursor;
            if (history != null) {
                seen.add(history);
            }
            page = next;
            row = 0;
            child = 0;
            emitted = false;
            digest = null;
        }
    }
}
