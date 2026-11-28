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

package org.apache.seatunnel.connectors.seatunnel.sevenshifts;

import org.apache.seatunnel.api.source.SourceSplit;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsHttp.failure;

/** Resumable traversal position. Holds IDs and cursors, never credentials or response records. */
final class SevenShiftsSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private static final int MAX_PAGES_PER_ENDPOINT = 10000;
    private static final int MAX_COMPANIES = 4096;
    private static final int MAX_USERS_PER_COMPANY = 100000;

    enum Step {
        COMPANIES,
        USERS,
        RECORDS,
        DONE
    }

    final String checkpointKey;
    Step step;
    String cursor;
    private int companyIndex;
    private int userIndex;
    private final List<Long> companies;
    private final List<Long> users;
    private final Set<Long> parentIds;
    private final Set<String> seenCursors;
    private int pageCount;

    SevenShiftsSplit(SevenShiftsConfig config) {
        checkpointKey = config.checkpointKey();
        step =
                config.resource.scope == SevenShiftsResource.Scope.ROOT
                        ? Step.RECORDS
                        : Step.COMPANIES;
        companies = new ArrayList<>();
        users = new ArrayList<>();
        parentIds = new HashSet<>();
        seenCursors = new HashSet<>();
    }

    private SevenShiftsSplit(SevenShiftsSplit source) {
        checkpointKey = source.checkpointKey;
        step = source.step;
        cursor = source.cursor;
        companyIndex = source.companyIndex;
        userIndex = source.userIndex;
        companies = new ArrayList<>(source.companies);
        users = new ArrayList<>(source.users);
        parentIds = new HashSet<>(source.parentIds);
        seenCursors = new HashSet<>(source.seenCursors);
        pageCount = source.pageCount;
    }

    @Override
    public String splitId() {
        return "7shifts-scan";
    }

    SevenShiftsSplit copy() {
        return new SevenShiftsSplit(this);
    }

    Long companyId() {
        return companyIndex < companies.size() ? companies.get(companyIndex) : null;
    }

    Long userId() {
        return userIndex < users.size() ? users.get(userIndex) : null;
    }

    /**
     * Validates the next cursor before a reader can publish the associated page. The digest keeps
     * the checkpoint bounded even when the service supplies long opaque cursors.
     */
    String validateNext(SevenShiftsResource resource, String next)
            throws SevenShiftsHttp.RequestFailure {
        if (next == null) {
            return null;
        }
        if (pageCount + 1 >= MAX_PAGES_PER_ENDPOINT) {
            throw failure(resource, "pagination exceeds 10000 pages for one endpoint");
        }
        try {
            String digest =
                    Base64.getEncoder()
                            .encodeToString(
                                    MessageDigest.getInstance("SHA-256")
                                            .digest(next.getBytes(StandardCharsets.UTF_8)));
            if (seenCursors.contains(digest)) {
                throw failure(resource, "pagination repeated a cursor");
            }
            return digest;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    void addParents(Set<Long> ids, SevenShiftsResource selected)
            throws SevenShiftsHttp.RequestFailure {
        List<Long> target = step == Step.COMPANIES ? companies : users;
        int limit = step == Step.COMPANIES ? MAX_COMPANIES : MAX_USERS_PER_COMPANY;
        ids.removeAll(parentIds);
        if (ids.size() > limit - target.size()) {
            throw failure(selected, "parent discovery exceeds the supported scan limit");
        }
        parentIds.addAll(ids);
        target.addAll(ids);
    }

    /** Advances after the whole page has been validated and emitted. */
    void advance(SevenShiftsResource resource, String next, String digest) {
        if (next != null) {
            cursor = next;
            seenCursors.add(digest);
            pageCount++;
            return;
        }
        cursor = null;
        seenCursors.clear();
        pageCount = 0;
        switch (step) {
            case COMPANIES:
                parentIds.clear();
                if (companies.isEmpty()) {
                    step = Step.DONE;
                } else {
                    step =
                            resource.scope == SevenShiftsResource.Scope.USER
                                    ? Step.USERS
                                    : Step.RECORDS;
                }
                break;
            case USERS:
                parentIds.clear();
                if (users.isEmpty()) {
                    nextCompany(resource);
                } else {
                    step = Step.RECORDS;
                }
                break;
            case RECORDS:
                if (resource.scope == SevenShiftsResource.Scope.USER
                        && ++userIndex < users.size()) {
                    return;
                }
                nextCompany(resource);
                break;
            default:
                throw new IllegalStateException("7shifts Source traversal is finished");
        }
    }

    private void nextCompany(SevenShiftsResource resource) {
        users.clear();
        userIndex = 0;
        companyIndex++;
        if (companyIndex >= companies.size()) {
            companies.clear();
            step = Step.DONE;
        } else {
            step = resource.scope == SevenShiftsResource.Scope.USER ? Step.USERS : Step.RECORDS;
        }
    }
}
