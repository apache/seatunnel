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

package org.apache.seatunnel.connectors.seatunnel.agilecrm;

import org.apache.seatunnel.api.source.SourceSplit;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmClient.fail;

/** A bounded scan position. Holds discovery IDs and cursor state, never credentials or records. */
final class AgileCrmTraversal implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private static final int MAX_PARENTS = 100000;
    private static final int MAX_CONTEXT_PAGES = 10000;
    private static final int MAX_SCAN_PAGES = 200000;

    final String stateKey;
    private final List<Long> parents;
    private final Set<Long> discovered;
    private final Set<String> cursors;
    boolean discovery;
    boolean finished;
    private String cursor;
    private int parentIndex;
    private int scopePages;
    private int scannedPages;

    AgileCrmTraversal(AgileCrmSettings config) {
        stateKey = config.stateKey();
        parents = new ArrayList<>();
        discovered = new HashSet<>();
        cursors = new HashSet<>();
        discovery = config.resource.parent != null;
    }

    private AgileCrmTraversal(AgileCrmTraversal old) {
        stateKey = old.stateKey;
        parents = new ArrayList<>(old.parents);
        discovered = new HashSet<>(old.discovered);
        cursors = new HashSet<>(old.cursors);
        discovery = old.discovery;
        finished = old.finished;
        cursor = old.cursor;
        parentIndex = old.parentIndex;
        scopePages = old.scopePages;
        scannedPages = old.scannedPages;
    }

    @Override
    public String splitId() {
        return "agilecrm-resource-list";
    }

    AgileCrmTraversal snapshot() {
        return new AgileCrmTraversal(this);
    }

    Long parentId() {
        return parentIndex < parents.size() && !discovery ? parents.get(parentIndex) : null;
    }

    AgileCrmResource.Request nextRequest(AgileCrmSettings config)
            throws AgileCrmClient.ReadFailure {
        if (scopePages == MAX_CONTEXT_PAGES || scannedPages == MAX_SCAN_PAGES) {
            throw fail(config.resource, "scan exceeded its page limit");
        }
        AgileCrmResource resource = discovery ? config.resource.parent : config.resource;
        return resource.list(config, parentId(), cursor);
    }

    /** Checks prospective progress before collecting any records in this page. */
    String checkNext(AgileCrmResource resource, String candidate)
            throws AgileCrmClient.ReadFailure {
        if (candidate == null) {
            return null;
        }
        try {
            String digest =
                    Base64.getEncoder()
                            .encodeToString(
                                    MessageDigest.getInstance("SHA-256")
                                            .digest(candidate.getBytes(StandardCharsets.UTF_8)));
            if (cursors.contains(digest)) {
                throw fail(resource, "pagination repeated a cursor");
            }
            return digest;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    void discover(AgileCrmResource resource, Set<Long> parentIds)
            throws AgileCrmClient.ReadFailure {
        parentIds.removeAll(discovered);
        if (parentIds.size() > MAX_PARENTS - parents.size()) {
            throw fail(resource, "parent discovery exceeded 100000 IDs");
        }
        parents.addAll(parentIds);
        discovered.addAll(parentIds);
    }

    void advance(AgileCrmResource resource, String next, String digest) {
        scannedPages++;
        cursor = next;
        if (cursor != null) {
            cursors.add(digest);
            scopePages++;
            return;
        }
        scopePages = 0;
        cursors.clear();
        if (discovery) {
            discovery = false;
            discovered.clear();
            finished = parents.isEmpty();
        } else if (resource.parent == null || ++parentIndex >= parents.size()) {
            parents.clear();
            finished = true;
        }
    }
}
