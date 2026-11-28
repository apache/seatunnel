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
package org.apache.seatunnel.connectors.seatunnel.babelforce;

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.HashSet;
import java.util.Set;

/**
 * Immutable progress of one Babelforce account and creation window.
 *
 * <p>Page positions, published row count and response digest avoid using creation timestamps as
 * continuation cursors. A whole group of calls in the same second remains addressable.
 */
final class BabelforceCheckpoint implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String fingerprint;
    final long until;
    final int page;
    final int pages;
    final long total;
    final long received;
    final Set<String> pageKeys;
    // The digest is present only when part of the current response has been emitted.
    final String partialDigest;
    final int emitted;
    final boolean done;

    BabelforceCheckpoint(BabelforceOptions options) {
        this(options.fingerprint, options.initialEnd(), 1, -1, -1L, 0L, Set.of(), null, 0, false);
    }

    private BabelforceCheckpoint(
            String fingerprint,
            long until,
            int page,
            int pages,
            long total,
            long received,
            Set<String> pageKeys,
            String partialDigest,
            int emitted,
            boolean done) {
        this.fingerprint = fingerprint;
        this.until = until;
        this.page = page;
        this.pages = pages;
        this.total = total;
        this.received = received;
        this.pageKeys = pageKeys;
        this.partialDigest = partialDigest;
        this.emitted = emitted;
        this.done = done;
    }

    /**
     * Records a complete validated provider page.
     *
     * <p>Hashes identify repeated page contents without retaining callback or call data.
     */
    BabelforceCheckpoint advance(
            int count, int reportedPages, long reportedTotal, String pageHash, boolean last) {
        if (last) {
            return new BabelforceCheckpoint(
                    fingerprint,
                    until,
                    page,
                    reportedPages,
                    reportedTotal,
                    received + count,
                    Set.of(),
                    null,
                    0,
                    true);
        }
        Set<String> history = new HashSet<>(pageKeys);
        if (pageHash != null) {
            history.add(pageHash);
        }
        return new BabelforceCheckpoint(
                fingerprint,
                until,
                page + 1,
                reportedPages,
                reportedTotal,
                received + count,
                Set.copyOf(history),
                null,
                0,
                false);
    }

    BabelforceCheckpoint partial(String digest, int offset) {
        return new BabelforceCheckpoint(
                fingerprint, until, page, pages, total, received, pageKeys, digest, offset, false);
    }

    @Override
    public String splitId() {
        return "babelforce-calls";
    }
}
