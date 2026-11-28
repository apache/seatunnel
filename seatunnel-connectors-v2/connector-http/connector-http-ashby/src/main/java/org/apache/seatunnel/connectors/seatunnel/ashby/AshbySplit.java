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

package org.apache.seatunnel.connectors.seatunnel.ashby;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Checkpointable state for the active provider pass and its nested cursor.
 *
 * <p>A final parent token remains pending until all descendants from that pass have been emitted.
 * The active token never changes while a pass or a partly emitted page is in flight.
 */
final class AshbySplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String fingerprint;
    // Requests throughout one pass use the same committed token.
    String syncToken;
    // History retains the final application-list token until the last descendant is emitted.
    String finalParentToken;
    String cursor;
    // Hashed continuation history detects cycles without retaining prior opaque values.
    Set<String> cursors = Set.of();
    String childCursor;
    Set<String> childCursors = Set.of();
    // Only the active discovery page is retained while its children are traversed.
    List<Parent> parents = List.of();
    int parentIndex;
    boolean finalParentPage;
    // This counts logical requests across both traversal levels in the current pass.
    int requests;
    // Save the absolute next-poll time so restoration respects the wait already scheduled.
    Instant readyAt;
    boolean done;
    // A mid-page restore must verify the reread before resuming the saved output offset.
    String pageDigest;
    int pageOffset;

    AshbySplit(AshbySettings settings) {
        fingerprint = settings.fingerprint;
        syncToken = settings.initialSyncToken;
    }

    private AshbySplit(AshbySplit saved) {
        fingerprint = saved.fingerprint;
        syncToken = saved.syncToken;
        finalParentToken = saved.finalParentToken;
        cursor = saved.cursor;
        cursors = saved.cursors;
        childCursor = saved.childCursor;
        childCursors = saved.childCursors;
        parents = saved.parents;
        parentIndex = saved.parentIndex;
        finalParentPage = saved.finalParentPage;
        requests = saved.requests;
        readyAt = saved.readyAt;
        done = saved.done;
        pageDigest = saved.pageDigest;
        pageOffset = saved.pageOffset;
    }

    AshbySplit copy() {
        return new AshbySplit(this);
    }

    void continuePage(String next, boolean child, AshbyResource resource) throws AshbyFailure {
        Set<String> history = child ? childCursors : cursors;
        String digest = AshbySettings.hash(next);
        if (history.contains(digest)) {
            throw new AshbyFailure(resource, "provider repeated a page cursor");
        }
        Set<String> visited = new HashSet<>(history);
        visited.add(digest);
        if (child) {
            childCursor = next;
            childCursors = Set.copyOf(visited);
        } else {
            cursor = next;
            cursors = Set.copyOf(visited);
        }
    }

    void finishChild() {
        childCursor = null;
        childCursors = Set.of();
        parentIndex++;
        if (parentIndex == parents.size()) {
            parentIndex = 0;
            parents = List.of();
        }
    }

    void finishPass(AshbySettings settings, String terminalToken) {
        syncToken = terminalToken;
        finalParentToken = null;
        cursor = null;
        cursors = Set.of();
        finalParentPage = false;
        requests = 0;
        done = settings.interval == 0;
        readyAt = done ? null : Instant.now().plusMillis(settings.interval);
    }

    @Override
    public String splitId() {
        return "ashby";
    }

    /**
     * Retains the bounded lineage required to finish child calls after restore.
     *
     * <p>The entire application or interview plan is never stored in the checkpoint.
     */
    static final class Parent implements Serializable {
        private static final long serialVersionUID = 1L;
        final String id;
        final String status;
        final String created;
        final String updated;
        final Boolean archived;

        Parent(String id, String status, String created, String updated, Boolean archived) {
            this.id = id;
            this.status = status;
            this.created = created;
            this.updated = updated;
            this.archived = archived;
        }
    }
}
