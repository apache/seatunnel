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

package org.apache.seatunnel.connectors.seatunnel.onehundredms;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsConfig.digest;

/**
 * A detached v2 checkpoint records the current request, parent traversal and accepted page prefix.
 * A partial page keeps its cursor until every included row is accepted; its count and fixed-size
 * rolling digest let restore validate the refetched prefix before skipping it. Credentials, decoded
 * response payloads and in-flight requests never enter checkpoints.
 */
final class OneHundredMsProgress implements SourceSplit {
    private static final long serialVersionUID = 2L;

    final String fingerprint;
    final Instant upperBound;
    int resourceIndex;
    int phase;
    boolean discoveryFinished;
    List<String> parents = new ArrayList<>();
    int parentIndex;
    long analyticsStartedAt;
    boolean finished;
    final Set<String> parentHistory = new HashSet<>();
    Scan data = new Scan();
    Scan discovery = new Scan();

    OneHundredMsProgress(String fingerprint, Instant upperBound) {
        this.fingerprint = fingerprint;
        this.upperBound = upperBound;
    }

    OneHundredMsProgress(OneHundredMsProgress source) {
        this(source.fingerprint, source.upperBound);
        resourceIndex = source.resourceIndex;
        phase = source.phase;
        discoveryFinished = source.discoveryFinished;
        parents = new ArrayList<>(source.parents);
        parentIndex = source.parentIndex;
        analyticsStartedAt = source.analyticsStartedAt;
        finished = source.finished;
        parentHistory.addAll(source.parentHistory);
        data = source.data.copy();
        discovery = source.discovery.copy();
    }

    @Override
    public String splitId() {
        return "100ms-resources";
    }

    void nextResource(int count) {
        finished = ++resourceIndex == count;
        phase = 0;
        discoveryFinished = false;
        parents.clear();
        parentIndex = 0;
        analyticsStartedAt = 0;
        parentHistory.clear();
        data = new Scan();
        discovery = new Scan();
    }

    /**
     * Cursor hashes catch retries and cycles without retaining the cursor chain in plain text. The
     * current page also carries only an accepted-row count and digest, never its decoded row
     * payloads.
     */
    static final class Scan implements Serializable {
        private static final long serialVersionUID = 2L;

        String current;
        String previousPage;
        int pages;
        int acceptedCount;
        String acceptedHash;
        final Set<String> visited = new HashSet<>();

        void validate(OneHundredMsResource resource, OneHundredMsResource.Page page, int max)
                throws IOException {
            if (pages >= max || (pages + 1 == max && page.next != null)) {
                throw resource.invalid("max_pages was reached before the endpoint finished");
            }
            if (page.next != null && visited.contains(digest(page.next))) {
                throw resource.invalid("continuation cursor repeated or cycled");
            }
            if (!page.rows.isEmpty() && page.fingerprint().equals(previousPage)) {
                throw resource.invalid("consecutive pages returned the same records");
            }
        }

        void advance(OneHundredMsResource.Page page, String fingerprint) {
            acceptedCount = 0;
            acceptedHash = null;
            pages++;
            current = page.next;
            if (!page.rows.isEmpty()) {
                previousPage = fingerprint;
            }
            if (current != null) {
                visited.add(digest(current));
            }
        }

        Scan copy() {
            Scan copy = new Scan();
            copy.current = current;
            copy.previousPage = previousPage;
            copy.pages = pages;
            copy.acceptedCount = acceptedCount;
            copy.acceptedHash = acceptedHash;
            copy.visited.addAll(visited);
            return copy;
        }
    }
}
