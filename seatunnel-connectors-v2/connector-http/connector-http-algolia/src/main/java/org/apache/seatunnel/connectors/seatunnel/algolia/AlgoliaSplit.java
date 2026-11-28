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
package org.apache.seatunnel.connectors.seatunnel.algolia;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/** Owns the frozen time interval and the exact position of one bounded export. */
final class AlgoliaSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    // Credentials can rotate. Only the extraction parameters and frozen end bind the split.
    final String scope;
    final Instant end;
    int group;
    Cursor root = new Cursor();
    int parent;
    int childIndex;
    boolean indexEmitted;
    Cursor child = new Cursor();
    // A saved parent offset is safe only when the refetched page contains the same indices.
    String parentPageDigest;

    AlgoliaSplit(String scope, Instant end) {
        this.scope = scope;
        this.end = end;
    }

    @Override
    public String splitId() {
        return "algolia-export";
    }

    AlgoliaSplit copy() {
        AlgoliaSplit out = new AlgoliaSplit(scope, end);
        out.group = group;
        out.root = root.copy();
        out.parent = parent;
        out.childIndex = childIndex;
        out.indexEmitted = indexEmitted;
        out.child = child.copy();
        out.parentPageDigest = parentPageDigest;
        return out;
    }

    void nextGroup() {
        group++;
        root = new Cursor();
        clearParentPage();
    }

    void nextParent() {
        parent++;
        childIndex = 0;
        indexEmitted = false;
        child = new Cursor();
    }

    void clearParentPage() {
        parent = 0;
        childIndex = 0;
        indexEmitted = false;
        child = new Cursor();
        parentPageDigest = null;
    }

    void validate(AlgoliaConfig config) {
        if (!config.scope.equals(scope)
                || end == null
                || !config.start.isBefore(end)
                || config.requestedEnd != null && !config.requestedEnd.equals(end)) {
            throw new IllegalArgumentException(
                    "Algolia checkpoint does not match the configured source scope");
        }
        if (group < 0
                || group > config.roots.size()
                || root == null
                || child == null
                || parent < 0
                || parent > config.indicesSize
                || childIndex < 0
                || childIndex > config.children.size()) {
            throw new IllegalArgumentException(
                    "Algolia checkpoint contains an invalid traversal position");
        }
        AlgoliaResource resource = group == config.roots.size() ? null : config.roots.get(group);
        root.validate(config, resource);
        child.validate(
                config,
                childIndex < config.children.size() ? config.children.get(childIndex) : null);
        boolean nested = resource == AlgoliaResource.INDICES && !config.children.isEmpty();
        if (!nested
                        && (parentPageDigest != null
                                || parent != 0
                                || childIndex != 0
                                || indexEmitted
                                || !child.initial())
                || nested
                        && ((parentPageDigest == null
                                        && (parent != 0 || childIndex != 0 || indexEmitted))
                                || (parentPageDigest != null
                                        && !AlgoliaPage.isHash(parentPageDigest))
                                || !indexEmitted && (childIndex != 0 || !child.initial())
                                || root.row != 0)) {
            throw new IllegalArgumentException(
                    "Algolia checkpoint contains an invalid parent or child position");
        }
    }

    /** Tracks page or log-offset progress and the digest of a partly emitted response. */
    static final class Cursor implements Serializable {
        private static final long serialVersionUID = 1L;

        int page;
        int offset;
        int row;
        int expectedPages = -1;
        int hitsPerPage = -1;
        long expectedHits = -1;
        Instant lastTimestamp;
        String activeDigest;
        String previousKeys;
        // Search and logs have bounded horizons, so identities from committed pages are retained
        // safely.
        Set<String> seenKeys;

        Cursor copy() {
            Cursor out = new Cursor();
            out.page = page;
            out.offset = offset;
            out.row = row;
            out.expectedPages = expectedPages;
            out.hitsPerPage = hitsPerPage;
            out.expectedHits = expectedHits;
            out.lastTimestamp = lastTimestamp;
            out.activeDigest = activeDigest;
            out.previousKeys = previousKeys;
            out.seenKeys = seenKeys == null ? null : new HashSet<>(seenKeys);
            return out;
        }

        void advance(AlgoliaResource resource, AlgoliaPage result) {
            page++;
            row = 0;
            activeDigest = null;
            expectedPages = result.pages;
            hitsPerPage = result.perPage;
            expectedHits = result.hits;
            previousKeys = result.keysDigest;
            if (resource == AlgoliaResource.LOGS) {
                offset += result.rows.size();
                lastTimestamp = result.rows.get(result.rows.size() - 1).timestamp;
            }
            if (resource == AlgoliaResource.LOGS || resource == AlgoliaResource.INDEXES_QUERY) {
                if (seenKeys == null) {
                    seenKeys = new HashSet<>();
                }
                for (AlgoliaPage.Row entry : result.rows) {
                    seenKeys.add(AlgoliaPage.hash(entry.id));
                }
            }
        }

        boolean initial() {
            return page == 0 && row == 0;
        }

        void validate(AlgoliaConfig config, AlgoliaResource resource) {
            if (page < 0
                    || page >= config.maxPages
                    || row < 0
                    || row > 1000
                    || (row == 0 ? activeDigest != null : !AlgoliaPage.isHash(activeDigest))
                    || resource == null && !initial()
                    || resource != AlgoliaResource.LOGS && (offset != 0 || lastTimestamp != null)
                    || resource == AlgoliaResource.LOGS
                            && (offset < 0
                                    || offset >= AlgoliaResource.LOG_HORIZON
                                    || (page == 0
                                            ? offset != 0 || lastTimestamp != null
                                            : offset == 0 || lastTimestamp == null))
                    || seenKeys != null && seenKeys.size() > AlgoliaPage.QUERY_LIMIT
                    || previousKeys != null && !AlgoliaPage.isHash(previousKeys)) {
                throw new IllegalArgumentException(
                        "Algolia checkpoint contains an invalid page state");
            }
        }
    }
}
