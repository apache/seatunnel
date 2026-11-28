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

package org.apache.seatunnel.connectors.seatunnel.boldsign;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignConfig.Resource;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignConfig.Window;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignJson.RawPage;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Plans one numbered page or document cursor-window transition.
 *
 * <p>A page's next state is applied only after its selected rows are committed to the collector.
 */
final class BoldSignPagination {
    private BoldSignPagination() {}

    /**
     * Validates the complete response and builds its logical output and continuation.
     *
     * <p>Creation-time filtering never decides whether paging continues. Cursor overlap uses every
     * provider row, including rows outside the requested creation window.
     */
    static Page prepare(
            BoldSignConfig config,
            Resource resource,
            Window window,
            String brand,
            Cursor request,
            RawPage raw)
            throws IOException {
        boolean documents = resource == Resource.DOCUMENTS;
        DocumentWindow tracker = documents ? new DocumentWindow(request.documents) : null;
        List<String> output = new ArrayList<>();
        List<String> all = new ArrayList<>();
        for (JsonNode row : raw.rows) {
            BoldSignJson.brand(row, resource, brand);
            boolean overlap = documents && tracker.observe(row);
            boolean inWindow = !resource.dated || BoldSignJson.selected(row, window);
            String json = BoldSignJson.encode(row);
            all.add(json);
            if (!overlap && inWindow) {
                output.add(json);
            }
        }
        Cursor next = null;
        if (documents
                && request.page * config.pageSize == BoldSignConfig.DOCUMENT_WINDOW_LIMIT
                && (raw.more || raw.rows.size() == config.pageSize)) {
            next = tracker.rollover(request.completed + 1);
        } else if (raw.more) {
            next =
                    new Cursor(
                            request.page + 1,
                            request.completed + 1,
                            documents ? tracker.snapshot() : null);
        } else if (documents) {
            tracker.requireOverlapComplete();
        }
        if (next != null && next.completed >= config.maxPages) {
            throw BoldSignJson.failure("listing requires more than max_pages");
        }
        String nextAnchor =
                next == null || next.documents == null
                        ? ""
                        : Objects.toString(next.documents.anchor, "");
        String continuation = next == null ? "terminal" : next.page + "|" + nextAnchor;
        return new Page(output, next, BoldSignJson.fingerprint(all, continuation));
    }

    /**
     * One reply with an explicit next request position.
     *
     * <p>Only the saved position and fingerprint are serialized by the reader.
     */
    static final class Page {
        final List<String> output;
        final Cursor next;
        final String digest;

        Page(List<String> output, Cursor next, String digest) {
            this.output = output;
            this.next = next;
            this.digest = digest;
        }
    }

    /**
     * Durable provider request position.
     *
     * <p>Completed counts across all cursor windows so a service cannot cause unlimited reads.
     */
    static final class Cursor implements Serializable {
        private static final long serialVersionUID = 1L;
        final int page;
        final int completed;
        final DocumentState documents;

        Cursor(int page, int completed, DocumentState documents) {
            this.page = page;
            this.completed = completed;
            this.documents = documents;
        }

        static Cursor first(Resource resource) {
            return new Cursor(
                    1, 0, resource == Resource.DOCUMENTS ? DocumentState.initial() : null);
        }

        Long anchor() {
            return documents == null ? null : documents.anchor;
        }
    }

    /**
     * The current provider cursor anchor and bounded document identity hashes used for overlap.
     *
     * <p>Both sets are bounded by the service's 10000-row numbered window.
     */
    static final class DocumentState implements Serializable {
        private static final long serialVersionUID = 1L;
        final Long anchor;
        final Long previousDistinct;
        final Long current;
        final List<String> tail;
        final Long overlapCursor;
        final List<String> overlap;
        final List<Long> seenAnchors;

        DocumentState(
                Long anchor,
                Long previousDistinct,
                Long current,
                List<String> tail,
                Long overlapCursor,
                List<String> overlap,
                List<Long> seenAnchors) {
            this.anchor = anchor;
            this.previousDistinct = previousDistinct;
            this.current = current;
            this.tail = List.copyOf(tail);
            this.overlapCursor = overlapCursor;
            this.overlap = List.copyOf(overlap);
            this.seenAnchors = List.copyOf(seenAnchors);
        }

        static DocumentState initial() {
            return new DocumentState(null, null, null, List.of(), null, List.of(), List.of());
        }
    }

    /**
     * Tracks the trailing equal-cursor group using only opaque ID hashes.
     *
     * <p>At 10000 rows, an earlier distinct cursor permits replay and deduplication of the trailing
     * group. Advancing directly at a tied cursor could omit unseen records of that group.
     */
    private static final class DocumentWindow {
        private final DocumentState initial;
        private final Set<String> tail;
        private final Set<String> overlap;
        private Long previousDistinct;
        private Long current;

        DocumentWindow(DocumentState initial) {
            this.initial = initial;
            tail = new LinkedHashSet<>(initial.tail);
            overlap = new LinkedHashSet<>(initial.overlap);
            previousDistinct = initial.previousDistinct;
            current = initial.current;
        }

        boolean observe(JsonNode row) throws IOException {
            String hash =
                    BoldSignJson.digest(BoldSignJson.id(row.path("documentId"), "documentId"));
            Long position = BoldSignJson.documentCursor(row);
            boolean duplicate = false;
            if (!overlap.isEmpty()) {
                if (!initial.overlapCursor.equals(position)) {
                    throw BoldSignJson.failure(
                            "document rollover no longer contains its saved overlap");
                }
                duplicate = overlap.remove(hash);
            }
            if (position == null) {
                previousDistinct = null;
                current = null;
                tail.clear();
                return duplicate;
            }
            if (!position.equals(current)) {
                if (current != null) {
                    previousDistinct = current;
                }
                current = position;
                tail.clear();
            }
            if (!tail.add(hash)) {
                throw BoldSignJson.failure("duplicate document in one cursor group");
            }
            return duplicate;
        }

        Cursor rollover(int completed) throws IOException {
            requireOverlapComplete();
            if (current == null
                    || previousDistinct == null
                    || tail.isEmpty()
                    || initial.seenAnchors.contains(previousDistinct)) {
                throw BoldSignJson.failure("document cursor has no distinct safe rollover anchor");
            }
            List<Long> seen = new ArrayList<>(initial.seenAnchors);
            seen.add(previousDistinct);
            return new Cursor(
                    1,
                    completed,
                    new DocumentState(
                            previousDistinct,
                            previousDistinct,
                            null,
                            List.of(),
                            current,
                            new ArrayList<>(tail),
                            seen));
        }

        void requireOverlapComplete() throws IOException {
            if (!overlap.isEmpty()) {
                throw BoldSignJson.failure(
                        "document rollover ended before its saved overlap was verified");
            }
        }

        DocumentState snapshot() {
            return new DocumentState(
                    initial.anchor,
                    previousDistinct,
                    current,
                    new ArrayList<>(tail),
                    overlap.isEmpty() ? null : initial.overlapCursor,
                    new ArrayList<>(overlap),
                    initial.seenAnchors);
        }
    }
}
