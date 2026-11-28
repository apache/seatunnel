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
package org.apache.seatunnel.connectors.seatunnel.box;

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Pending Box traversal, parent positions and partially published response state.
 *
 * <p>Checkpoints keep only scoped IDs, paging state and hashes. Document content, model results and
 * credentials do not enter the split.
 */
final class BoxSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    static final int MAX_PENDING = 10000;

    final String scope;
    final List<BoxWork> work;
    final int requests;
    final int extractionItems;
    final Set<String> visitedItems;
    final String partialFingerprint;
    final int published;

    BoxSplit(BoxConfig config) {
        this(
                config.scope,
                BoxWork.start(config),
                0,
                0,
                Set.of("folder:" + config.folderId),
                null,
                0);
    }

    private BoxSplit(
            String scope,
            List<BoxWork> work,
            int requests,
            int extractionItems,
            Set<String> visitedItems,
            String partialFingerprint,
            int published) {
        this.scope = scope;
        this.work = work;
        this.requests = requests;
        this.extractionItems = extractionItems;
        this.visitedItems = visitedItems;
        this.partialFingerprint = partialFingerprint;
        this.published = published;
    }

    /**
     * Marks the current response complete and puts its children ahead of its continuation.
     *
     * <p>Discovered file and folder IDs remain bounded by the number of entries configured for
     * extraction.
     */
    BoxSplit advance(List<BoxWork> added, int used, int newItems, Set<String> newIdentities)
            throws BoxReadException {
        int remaining = work.size() - 1;
        if (added.size() > MAX_PENDING - remaining) {
            throw work.get(0).failure("pending Box work exceeds the account traversal limit");
        }
        List<BoxWork> next = new ArrayList<>(added.size() + remaining);
        next.addAll(added);
        next.addAll(work.subList(1, work.size()));
        Set<String> visited = visitedItems;
        if (!newIdentities.isEmpty()) {
            Set<String> cumulative = new HashSet<>(visitedItems);
            cumulative.addAll(newIdentities);
            visited = Set.copyOf(cumulative);
        }
        return new BoxSplit(
                scope,
                List.copyOf(next),
                requests + used,
                extractionItems + newItems,
                visited,
                null,
                0);
    }

    BoxSplit partial(String fingerprint, int count) {
        return new BoxSplit(
                scope, work, requests, extractionItems, visitedItems, fingerprint, count);
    }

    @Override
    public String splitId() {
        return "box-resources";
    }
}
