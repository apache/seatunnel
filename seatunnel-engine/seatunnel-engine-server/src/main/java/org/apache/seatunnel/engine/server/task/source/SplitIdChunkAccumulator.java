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

package org.apache.seatunnel.engine.server.task.source;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Worker-budgeted accumulator for chunked Source split identifiers. */
final class SplitIdChunkAccumulator implements AutoCloseable {
    private final String groupId;
    private final int chunkCount;
    private final long maxBytes;
    private final ManagedSourceMemoryBudget workerBudget;
    private final Map<Integer, List<String>> chunks = new HashMap<>();
    private long trackedBytes;
    private boolean closed;

    SplitIdChunkAccumulator(
            String groupId, int chunkCount, long maxBytes, ManagedSourceMemoryBudget workerBudget) {
        if (groupId == null
                || groupId.trim().isEmpty()
                || groupId.length() > SourceCommandEnvelope.MAX_IDENTIFIER_LENGTH) {
            throw new IllegalArgumentException("Source split-id group identifier is invalid");
        }
        if (chunkCount <= 0 || chunkCount > SourceCommandEnvelope.MAX_CHUNK_COUNT) {
            throw new IllegalArgumentException("Source split-id chunk count is invalid");
        }
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("Source split-id byte limit must be positive");
        }
        if (workerBudget == null) {
            throw new IllegalArgumentException("Source split-id worker budget must not be null");
        }
        this.groupId = groupId;
        this.chunkCount = chunkCount;
        this.maxBytes = maxBytes;
        this.workerBudget = workerBudget;
    }

    void add(String currentGroupId, int currentChunkCount, int chunkIndex, List<String> splitIds) {
        if (closed) {
            throw new IllegalStateException("Source split-id accumulator is closed");
        }
        if (!groupId.equals(currentGroupId)
                || chunkCount != currentChunkCount
                || chunkIndex < 0
                || chunkIndex >= chunkCount
                || splitIds == null) {
            throw new IllegalArgumentException("Source split-id chunk metadata is inconsistent");
        }
        List<String> copied = new ArrayList<>(splitIds.size());
        long addedBytes = 0L;
        for (String splitId : splitIds) {
            if (splitId == null || splitId.trim().isEmpty()) {
                throw new IllegalArgumentException("Source split identifier is invalid");
            }
            addedBytes =
                    Math.addExact(
                            addedBytes,
                            64L + Integer.BYTES + splitId.getBytes(StandardCharsets.UTF_8).length);
            copied.add(splitId);
        }
        List<String> immutable = Collections.unmodifiableList(copied);
        List<String> existing = chunks.get(chunkIndex);
        if (existing != null) {
            if (!existing.equals(immutable)) {
                throw new IllegalStateException("Conflicting duplicate Source split-id chunk");
            }
            return;
        }
        if (addedBytes > maxBytes - trackedBytes) {
            throw new IllegalStateException("Source split-id chunks exceed configured byte limit");
        }
        if (!workerBudget.tryReserve(addedBytes)) {
            throw new IllegalStateException(
                    "Worker Source memory budget is exhausted by split-id chunks");
        }
        chunks.put(chunkIndex, immutable);
        trackedBytes += addedBytes;
    }

    boolean complete() {
        return chunks.size() == chunkCount;
    }

    Set<String> splitIds() {
        if (!complete()) {
            throw new IllegalStateException("Source split-id chunks are incomplete");
        }
        Set<String> splitIds = new LinkedHashSet<>();
        for (int index = 0; index < chunkCount; index++) {
            List<String> chunk = chunks.get(index);
            if (chunk == null) {
                throw new IllegalStateException("Source split-id chunks have a gap");
            }
            splitIds.addAll(chunk);
        }
        return splitIds;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (trackedBytes > 0) {
            workerBudget.release(trackedBytes);
        }
        trackedBytes = 0L;
        chunks.clear();
    }
}
