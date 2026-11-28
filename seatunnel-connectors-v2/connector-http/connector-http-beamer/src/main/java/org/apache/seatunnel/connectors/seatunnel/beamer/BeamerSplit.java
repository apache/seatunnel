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
package org.apache.seatunnel.connectors.seatunnel.beamer;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Immutable progress for a single NPS feedback scan.
 *
 * <p>The date boundary and page position survive recovery without retaining raw feedback.
 */
final class BeamerSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final Instant windowEnd;
    final int page;
    final boolean done;
    // Page identity fingerprints detect that the provider ignored or repeated a page number.
    final Set<String> seenPages;
    // Exact known IDs from the preceding nonempty page detect immediate pagination overlap.
    final Set<String> previousIds;
    final String partialDigest;
    final int published;

    BeamerSplit(BeamerConfig config) {
        this(config.scope, config.initialEnd(), 0, false, Set.of(), Set.of(), null, 0);
    }

    private BeamerSplit(
            String scope,
            Instant windowEnd,
            int page,
            boolean done,
            Set<String> seenPages,
            Set<String> previousIds,
            String partialDigest,
            int published) {
        this.scope = scope;
        this.windowEnd = windowEnd;
        this.page = page;
        this.done = done;
        this.seenPages = seenPages;
        this.previousIds = previousIds;
        this.partialDigest = partialDigest;
        this.published = published;
    }

    /**
     * Moves to the next request after a verified nonempty page is fully published.
     *
     * <p>An empty array is the only terminator, so filtered and short pages still advance.
     */
    BeamerSplit advance(String identity, Set<String> ids) {
        Set<String> completed = new HashSet<>(seenPages);
        completed.add(identity);
        return new BeamerSplit(
                scope, windowEnd, page + 1, false, Set.copyOf(completed), Set.copyOf(ids), null, 0);
    }

    BeamerSplit finish() {
        return new BeamerSplit(scope, windowEnd, page, true, Set.of(), Set.of(), null, 0);
    }

    BeamerSplit partial(String digest, int outputCount) {
        return new BeamerSplit(
                scope, windowEnd, page, false, seenPages, previousIds, digest, outputCount);
    }

    @Override
    public String splitId() {
        return "beamer-nps";
    }
}
