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
package org.apache.seatunnel.connectors.seatunnel.bluetally;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Frozen update window and immutable offset progress across the selected list operations.
 *
 * <p>The active page fingerprint and number of emitted rows are sufficient to resume a partial page
 * without retaining any raw records.
 */
final class BlueTallySplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final Instant end;
    final int resource;
    final int offset;
    final int pages;
    final Instant lastUpdated;
    // Bounded page identities detect reuse even after an intervening page.
    final Set<String> seenPages;
    final Set<String> previousIds;
    final String partialFingerprint;
    final int published;

    BlueTallySplit(BlueTallyConfig config) {
        this(config.scope, config.initialEnd(), 0, 0, 0, null, Set.of(), Set.of(), null, 0);
    }

    private BlueTallySplit(
            String scope,
            Instant end,
            int resource,
            int offset,
            int pages,
            Instant lastUpdated,
            Set<String> seenPages,
            Set<String> previousIds,
            String partialFingerprint,
            int published) {
        this.scope = scope;
        this.end = end;
        this.resource = resource;
        this.offset = offset;
        this.pages = pages;
        this.lastUpdated = lastUpdated;
        this.seenPages = seenPages;
        this.previousIds = previousIds;
        this.partialFingerprint = partialFingerprint;
        this.published = published;
    }

    /**
     * Advances by the raw API row count only after all selected rows are published.
     *
     * <p>Filtered rows still occupy positions in the provider collection.
     */
    BlueTallySplit next(int count, Instant last, String pageIdentity, Set<String> ids) {
        Set<String> visited = new HashSet<>(seenPages);
        visited.add(pageIdentity);
        return new BlueTallySplit(
                scope,
                end,
                resource,
                offset + count,
                pages + 1,
                last,
                Set.copyOf(visited),
                Set.copyOf(ids),
                null,
                0);
    }

    BlueTallySplit finishResource() {
        return new BlueTallySplit(
                scope, end, resource + 1, 0, 0, null, Set.of(), Set.of(), null, 0);
    }

    BlueTallySplit partial(String fingerprint, int count) {
        return new BlueTallySplit(
                scope,
                end,
                resource,
                offset,
                pages,
                lastUpdated,
                seenPages,
                previousIds,
                fingerprint,
                count);
    }

    @Override
    public String splitId() {
        return "bluetally-inventory";
    }
}
