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

package org.apache.seatunnel.connectors.seatunnel.aha;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Checkpoint cursor for a single resource crawl. Stores only numeric pagination, one page of parent
 * IDs, and bounded hashes of completed page signatures and parent identifiers.
 */
final class AhaProgress implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String fingerprint;
    int index;
    int parentIndex;
    boolean discoveryDone;
    boolean finished;
    Scan data = new Scan();
    Scan discovery = new Scan();
    List<String> parents = new ArrayList<>();
    final Set<String> discoveredIds = new HashSet<>();

    AhaProgress(String fingerprint) {
        this.fingerprint = fingerprint;
    }

    AhaProgress(AhaProgress saved) {
        this(saved.fingerprint);
        index = saved.index;
        parentIndex = saved.parentIndex;
        discoveryDone = saved.discoveryDone;
        finished = saved.finished;
        data = new Scan(saved.data);
        discovery = new Scan(saved.discovery);
        parents = new ArrayList<>(saved.parents);
        discoveredIds.addAll(saved.discoveredIds);
    }

    @Override
    public String splitId() {
        return "aha-v1-crawl";
    }

    void nextResource(int size) {
        finished = ++index == size;
        parentIndex = 0;
        discoveryDone = false;
        data = new Scan();
        discovery = new Scan();
        parents.clear();
        discoveredIds.clear();
    }

    /** Validates a consistent provider count and page index across one parent or list traversal. */
    static final class Scan implements Serializable {
        private static final long serialVersionUID = 1L;
        int page = 1;
        private int pages = -1;
        private int total = -1;
        private int delivered;
        private final Set<String> seenPages = new HashSet<>();

        Scan() {}

        Scan(Scan saved) {
            page = saved.page;
            pages = saved.pages;
            total = saved.total;
            delivered = saved.delivered;
            seenPages.addAll(saved.seenPages);
        }

        void validate(AhaResource api, AhaResource.Page current, AhaConfig config)
                throws IOException {
            if (current.index != page) {
                throw api.problem("provider current_page differs from the request");
            }
            if (current.totalPages > config.maxPages
                    || (long) current.totalRows > (long) config.maxPages * config.pageSize) {
                throw api.problem("pagination exceeds max_pages");
            }
            if (pages >= 0 && (pages != current.totalPages || total != current.totalRows)) {
                throw api.problem("provider pagination totals changed during the scan");
            }
            int rows = current.records.size();
            if (current.totalRows == 0) {
                if (page != 1 || current.totalPages > 1 || rows != 0) {
                    throw api.problem("invalid empty collection pagination");
                }
                return;
            }
            if (rows == 0) {
                throw api.problem("unexpected empty page before pagination completed");
            }
            if (current.totalPages < page
                    || current.totalPages > current.totalRows
                    || delivered + rows > current.totalRows
                    || (current.last() && delivered + rows != current.totalRows)
                    || (!current.last() && delivered + rows >= current.totalRows)) {
                throw api.problem("records disagree with provider pagination totals");
            }
            if (seenPages.contains(current.fingerprint)) {
                throw api.problem("provider repeated a page's record identities");
            }
        }

        void advance(AhaResource.Page result) {
            pages = result.totalPages;
            total = result.totalRows;
            delivered += result.records.size();
            seenPages.add(result.fingerprint);
            page++;
        }
    }
}
