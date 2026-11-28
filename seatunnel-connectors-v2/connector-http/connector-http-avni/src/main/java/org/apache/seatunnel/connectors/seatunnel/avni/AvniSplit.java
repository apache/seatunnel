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
package org.apache.seatunnel.connectors.seatunnel.avni;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Checkpoint for an ordered batch over the selected Avni resources.
 *
 * <p>The fixed audit window, page fingerprint and committed row distinguish updates that share an
 * audit millisecond. Credentials and records stay outside the split.
 */
final class AvniSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private final String scope;
    final Instant start;
    final Instant end;

    // The selected resource and page stay fixed until every row in the page is committed.
    int resource;
    int page;
    int row;
    String digest;
    boolean finished;

    // Avni's page count and effective size should remain consistent within one resource scan.
    int totalPages = -1;
    int effectiveSize;
    Instant previousTime;
    Set<String> previousIds = Set.of();

    AvniSplit(String scope, Instant start, Instant end) {
        this.scope = scope;
        this.start = start;
        this.end = end;
        finished = !start.isBefore(end);
    }

    @Override
    public String splitId() {
        return "avni-audit-scan";
    }

    boolean finished() {
        return finished;
    }

    /**
     * Detaches the row and page positions under the reader's checkpoint lock.
     *
     * <p>Previous page identities are immutable hashes and can be shared safely.
     */
    AvniSplit copy() {
        AvniSplit saved = new AvniSplit(scope, start, end);
        saved.resource = resource;
        saved.page = page;
        saved.row = row;
        saved.digest = digest;
        saved.finished = finished;
        saved.totalPages = totalPages;
        saved.effectiveSize = effectiveSize;
        saved.previousTime = previousTime;
        saved.previousIds = previousIds;
        return saved;
    }

    void validate(AvniConfig config) {
        if (!config.scope.equals(scope)
                || !config.start.equals(start)
                || end == null
                || resource < 0
                || resource > config.resources.size()
                || !finished && (resource == config.resources.size() || !start.isBefore(end))
                || page < 0
                || row < 0
                || row > config.pageSize
                || row > 0 && digest == null
                || totalPages < -1
                || effectiveSize < 0
                || effectiveSize > config.pageSize
                || page > 0 && (totalPages <= page || effectiveSize == 0)
                || previousIds == null) {
            throw new IllegalStateException(
                    "Avni checkpoint has incompatible scan options or state");
        }
    }

    /**
     * Commits a completed page before opening another request.
     *
     * <p>The provider offset advances within the original audit window. Advancing the lower time
     * bound at this point could skip other records with the same timestamp.
     */
    void advance(AvniApi.Page received, AvniConfig config) {
        row = 0;
        digest = null;
        if (page + 1 < received.totalPages) {
            ++page;
            previousTime = received.tail;
            previousIds =
                    received.rows.stream()
                            .map(value -> AvniConfig.hash(value.id))
                            .collect(Collectors.toUnmodifiableSet());
        } else {
            ++resource;
            page = 0;
            totalPages = -1;
            effectiveSize = 0;
            previousTime = null;
            previousIds = Set.of();
            finished = resource == config.resources.size();
        }
    }
}
