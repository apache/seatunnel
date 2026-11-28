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

package org.apache.seatunnel.connectors.seatunnel.datadog;

import org.apache.seatunnel.api.source.SourceSplit;

import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Versioned logical progress. A response, running request and credential are never serialized.
 * Readers refetch the current page and verify its emitted prefix after restoration.
 */
final class DatadogSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private static final int VERSION = 1;
    // Reject a different logical checkpoint contract before interpreting its progress fields.
    private final int version;
    // A query/account digest binds progress to its scope without serializing plaintext keys.
    private final byte[] identity;
    // The coordinator freezes this exclusive event bound once; catalogs leave it null.
    final Instant end;
    // Inclusive start of the active event window; null identifies a catalog scan.
    Instant from;
    // Endpoint-defined page number, row offset or last monitor ID, never an emitted-row count.
    long position;
    // Opaque token identifying the current page, reused verbatim when that page is restored.
    String cursor;
    // Counts continuing pages in the active resource/window to bound provider pagination.
    int page;
    // Rows already emitted from this page after time filtering, not raw provider array offsets.
    int prefix;
    // Ordered hash of emitted rows only; refetch must match before skipping the committed prefix.
    byte[] prefixDigest = new byte[0];
    // Hashed successor cursors detect cycles without keeping raw tokens from earlier pages.
    final Set<String> seen = new HashSet<>();
    // Terminal state remains checkpointable so restore cannot restart an already finished scan.
    boolean complete;

    DatadogSplit(byte[] identity, Instant from, Instant end) {
        this.version = VERSION;
        this.identity = identity;
        this.from = from;
        this.end = end;
        this.complete = end != null && from.equals(end);
    }

    private DatadogSplit(DatadogSplit other, boolean includeSeen) {
        version = other.version;
        identity = other.identity.clone();
        end = other.end;
        from = other.from;
        position = other.position;
        cursor = other.cursor;
        page = other.page;
        prefix = other.prefix;
        prefixDigest = other.prefixDigest.clone();
        if (includeSeen) {
            seen.addAll(other.seen);
        }
        complete = other.complete;
    }

    boolean matches(byte[] scope) {
        return version == VERSION && MessageDigest.isEqual(identity, scope);
    }

    /**
     * Isolates checkpoint ownership, including cycle history, from subsequent reader mutations.
     * Mutable arrays and sets must not be shared with a saved snapshot.
     */
    DatadogSplit copy() {
        return new DatadogSplit(this, true);
    }

    /**
     * The I/O worker needs page/prefix coordinates, but never mutates or owns cursor-cycle history.
     */
    DatadogSplit requestView() {
        return new DatadogSplit(this, false);
    }

    /**
     * Adjacent event windows share one boundary; local filtering makes that boundary exclusive. The
     * final window is capped at the coordinator-frozen end.
     */
    Instant until(int days) {
        if (from == null) {
            return null;
        }
        Instant candidate = from.plus(Duration.ofDays(days));
        return candidate.isBefore(end) ? candidate : end;
    }

    @Override
    public String splitId() {
        return "datadog-scan";
    }
}
