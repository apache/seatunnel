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
package org.apache.seatunnel.connectors.seatunnel.assemblyai;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Checkpoint state for a bounded AssemblyAI transcript scan.
 *
 * <p>It records the exact active page, transcript, selected resource, and committed child row.
 * Provider documents, signed media URLs and authentication material stay outside state.
 */
final class AssemblyAISplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private final String scope;

    // Frozen creation bounds apply to summaries and all selected children.
    final Instant start;
    final Instant end;

    // Only the validated before_id is needed to reconstruct a list request.
    final Set<String> seenCursors = new HashSet<>();
    String before;
    Instant previousTail;
    List<String> previousIds = List.of();
    int pageNumber;
    int parent;
    String pageDigest;

    // Parent ID, resource and row together distinguish separate child arrays.
    String parentId;
    int resource;
    int childRow;
    String childDigest;
    boolean finished;

    AssemblyAISplit(String scope, Instant start, Instant end) {
        this.scope = scope;
        this.start = start;
        this.end = end;
        finished = start.isAfter(end);
    }

    @Override
    public String splitId() {
        return "assemblyai-transcript-scan";
    }

    /**
     * Detaches checkpoint fields and the pagination guard from the live reader.
     *
     * <p>Ancestor context contains only validated IDs, timestamps and safe fingerprints.
     */
    AssemblyAISplit copy() {
        AssemblyAISplit saved = new AssemblyAISplit(scope, start, end);
        saved.seenCursors.addAll(seenCursors);
        saved.before = before;
        saved.previousTail = previousTail;
        saved.previousIds = previousIds;
        saved.pageNumber = pageNumber;
        saved.parent = parent;
        saved.pageDigest = pageDigest;
        saved.parentId = parentId;
        saved.resource = resource;
        saved.childRow = childRow;
        saved.childDigest = childDigest;
        saved.finished = finished;
        return saved;
    }

    void validate(AssemblyAIConfig config) {
        if (!config.scope.equals(scope)
                || start == null
                || end == null
                || pageNumber < 0
                || parent < 0
                || parent > config.pageSize
                || resource < 0
                || resource > config.resources.size()
                || childRow < 0
                || childRow > 0 && (childDigest == null || parentId == null)) {
            throw new IllegalStateException(
                    "AssemblyAI checkpoint has incompatible scan options or state");
        }
    }

    void nextResource() {
        ++resource;
        childRow = 0;
        childDigest = null;
    }

    void nextParent() {
        ++parent;
        parentId = null;
        resource = 0;
        childRow = 0;
        childDigest = null;
    }

    /**
     * Commits the exact next older page and guards against a repeated cursor.
     *
     * <p>Dropping the current page identity is safe after every parent in it is processed.
     */
    void nextPage(AssemblyAIApi.Page page, int maximum) {
        if (pageNumber + 1 >= maximum || !seenCursors.add(page.older)) {
            throw AssemblyAIResource.TRANSCRIPTS.failure(
                    "pagination repeated a cursor or exceeded max_pages");
        }
        before = page.older;
        previousTail = page.tail;
        previousIds =
                page.rows.isEmpty()
                        ? previousIds
                        : page.rows.stream()
                                .map(item -> item.id)
                                .collect(Collectors.toUnmodifiableList());
        ++pageNumber;
        parent = 0;
        pageDigest = null;
    }
}
