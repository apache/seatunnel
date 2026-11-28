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
package org.apache.seatunnel.connectors.seatunnel.bugsnag;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * One request in a bounded, parent-scoped traversal.
 *
 * <p>The work stores approved cursor values and compact fingerprints rather than continuation URLs
 * or response records. Other scopes start with empty pagination history.
 */
final class BugSnagWork implements Serializable {
    private static final long serialVersionUID = 1L;

    final BugSnagResource resource;
    final String organization;
    final String project;
    final String search;
    final int page;
    final String offset;
    final String base;
    final int records;
    final List<String> previousIds;
    private final List<String> seenOffsets;
    private final List<String> seenPages;

    BugSnagWork(BugSnagResource resource, String organization, String project, String search) {
        this(
                resource,
                organization,
                project,
                search,
                1,
                resource == BugSnagResource.RELEASES ? "0" : null,
                null,
                0,
                List.of(),
                List.of(),
                List.of());
    }

    private BugSnagWork(
            BugSnagResource resource,
            String organization,
            String project,
            String search,
            int page,
            String offset,
            String base,
            int records,
            List<String> previousIds,
            List<String> seenOffsets,
            List<String> seenPages) {
        this.resource = resource;
        this.organization = organization;
        this.project = project;
        this.search = search;
        this.page = page;
        this.offset = offset;
        this.base = base;
        this.records = records;
        this.previousIds = List.copyOf(previousIds);
        this.seenOffsets = List.copyOf(seenOffsets);
        this.seenPages = List.copyOf(seenPages);
    }

    void validatePage(List<String> ids) throws BugSnagReadException {
        if (!ids.isEmpty() && Collections.binarySearch(seenPages, pageIdentity(ids)) >= 0) {
            throw failure("provider repeated an earlier page");
        }
    }

    BugSnagWork continueAt(String nextOffset, String nextBase, List<String> ids)
            throws BugSnagReadException {
        if (Objects.equals(nextOffset, offset)) {
            throw failure("continuation offset did not advance");
        }
        List<String> offsets =
                addOrdered(
                        seenOffsets,
                        BugSnagConfig.digest(nextOffset),
                        "provider repeated a continuation offset");
        List<String> pages =
                ids.isEmpty()
                        ? seenPages
                        : addOrdered(
                                seenPages, pageIdentity(ids), "provider repeated an earlier page");
        return new BugSnagWork(
                resource,
                organization,
                project,
                search,
                page + 1,
                nextOffset,
                nextBase,
                records + ids.size(),
                ids,
                offsets,
                pages);
    }

    private List<String> addOrdered(List<String> values, String value, String message)
            throws BugSnagReadException {
        int position = Collections.binarySearch(values, value);
        if (position >= 0) {
            throw failure(message);
        }
        List<String> next = new ArrayList<>(values);
        next.add(-position - 1, value);
        return next;
    }

    private static String pageIdentity(List<String> ids) {
        List<String> sorted = new ArrayList<>(ids);
        Collections.sort(sorted);
        return BugSnagConfig.digest(String.join("", sorted));
    }

    BugSnagReadException failure(String detail) {
        return new BugSnagReadException(resource.id + " page " + page, detail);
    }
}
