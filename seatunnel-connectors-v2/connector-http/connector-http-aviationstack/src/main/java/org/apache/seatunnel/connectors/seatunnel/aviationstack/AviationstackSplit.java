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
package org.apache.seatunnel.connectors.seatunnel.aviationstack;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Immutable progress for one ordered list of Aviationstack catalogs.
 *
 * <p>Offset and page digests are scoped to the current catalog. A frozen delivery window is shared
 * by the entire scan and survives recovery.
 */
final class AviationstackSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final Instant windowEnd;
    final int resourceIndex;
    final long offset;
    final long total;
    final int pages;
    // Retained page ID digests catch repetitions even when metadata claims a new offset.
    final Set<String> seenPages;
    // A partial page retains a digest and row count instead of the raw response.
    final String partialDigest;
    final int published;

    AviationstackSplit(AviationstackConfig config) {
        this(config.scope, config.initialEnd(), 0, 0L, -1L, 0, Set.of(), null, 0);
    }

    private AviationstackSplit(
            String scope,
            Instant windowEnd,
            int resourceIndex,
            long offset,
            long total,
            int pages,
            Set<String> seenPages,
            String partialDigest,
            int published) {
        this.scope = scope;
        this.windowEnd = windowEnd;
        this.resourceIndex = resourceIndex;
        this.offset = offset;
        this.total = total;
        this.pages = pages;
        this.seenPages = seenPages;
        this.partialDigest = partialDigest;
        this.published = published;
    }

    /**
     * Advances the absolute offset after validation or moves to the next catalog.
     *
     * <p>Catalog transitions clear pagination metadata but carry the delivery window forward.
     */
    AviationstackSplit advance(long nextOffset, long advertisedTotal, String pageKeyDigest) {
        if (nextOffset == advertisedTotal) {
            return new AviationstackSplit(
                    scope, windowEnd, resourceIndex + 1, 0L, -1L, 0, Set.of(), null, 0);
        }
        Set<String> completed = new HashSet<>(seenPages);
        completed.add(pageKeyDigest);
        return new AviationstackSplit(
                scope,
                windowEnd,
                resourceIndex,
                nextOffset,
                advertisedTotal,
                pages + 1,
                Set.copyOf(completed),
                null,
                0);
    }

    AviationstackSplit partial(String digest, int outputCount) {
        return new AviationstackSplit(
                scope,
                windowEnd,
                resourceIndex,
                offset,
                total,
                pages,
                seenPages,
                digest,
                outputCount);
    }

    @Override
    public String splitId() {
        return "aviationstack-catalogs";
    }
}
