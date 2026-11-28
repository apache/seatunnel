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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Checkpointable source position for one resource and marketplace.
 *
 * <p>The worker may edit only a detached copy. A committed position contains provider IDs and
 * pagination cursors, never access tokens, signed URLs, or document contents.
 */
final class SpPosition implements SourceSplit {
    private static final long serialVersionUID = 1L;
    final String fingerprint;
    final Instant cutoff;
    Instant from;
    Instant until;
    Phase phase;
    int requests;
    String cursor;
    Set<String> cursorHashes = Set.of();
    // A generated report must not reappear on a later page after output has advanced.
    Set<String> generatedReportHashes = Set.of();
    List<String> parents = List.of();
    Map<String, String> parentDates = Map.of();
    int parentIndex;
    boolean parentFinished;
    String childCursor;
    Set<String> childHashes = Set.of();
    String report;
    String document;
    Instant reportDeadline;
    Instant nextPoll;
    Instant reportDataEnd;
    boolean submitted;
    String documentDigest;
    int documentOffset;

    /**
     * The request or output transition stored by the reader.
     *
     * <p>A submitted report, its polling phase, and its decoded document position can be restored
     * independently of authentication or the expired download link.
     */
    enum Phase {
        API,
        LOOKUP,
        CREATE,
        GENERATED_LIST,
        POLL,
        DOCUMENT,
        DONE
    }

    SpPosition(String fingerprint, Instant cutoff, Instant from, Instant until, Phase phase) {
        this.fingerprint = fingerprint;
        this.cutoff = cutoff;
        this.from = from;
        this.until = until;
        this.phase = phase;
    }

    SpPosition copy() {
        SpPosition value = new SpPosition(fingerprint, cutoff, from, until, phase);
        value.requests = requests;
        value.cursor = cursor;
        value.cursorHashes = cursorHashes;
        value.generatedReportHashes = generatedReportHashes;
        value.parents = parents;
        value.parentDates = parentDates;
        value.parentIndex = parentIndex;
        value.parentFinished = parentFinished;
        value.childCursor = childCursor;
        value.childHashes = childHashes;
        value.report = report;
        value.document = document;
        value.reportDeadline = reportDeadline;
        value.nextPoll = nextPoll;
        value.reportDataEnd = reportDataEnd;
        value.submitted = submitted;
        value.documentDigest = documentDigest;
        value.documentOffset = documentOffset;
        return value;
    }

    void setCursor(String token, boolean child) throws SpFailure {
        Set<String> previous = child ? childHashes : cursorHashes;
        Set<String> saved = previous;
        if (token != null) {
            String key = SpSettings.hash(token);
            if (previous.contains(key)) {
                throw new SpFailure("pagination", "provider repeated its cursor");
            }
            Set<String> next = new HashSet<>(previous);
            next.add(key);
            saved = Set.copyOf(next);
        }
        if (child) {
            childCursor = token;
            childHashes = token == null ? Set.of() : saved;
        } else {
            cursor = token;
            cursorHashes = saved;
        }
    }

    void resetPages() {
        cursor = null;
        cursorHashes = Set.of();
        generatedReportHashes = Set.of();
        parents = List.of();
        parentDates = Map.of();
        parentIndex = 0;
        parentFinished = false;
        childCursor = null;
        childHashes = Set.of();
    }

    void resetReport() {
        report = null;
        document = null;
        reportDeadline = null;
        nextPoll = null;
        reportDataEnd = null;
        submitted = false;
        documentDigest = null;
        documentOffset = 0;
    }

    @Override
    public String splitId() {
        return "amazon-seller-partner";
    }
}
