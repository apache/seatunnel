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
package org.apache.seatunnel.connectors.seatunnel.canny;

/**
 * Declares the fixed Canny list operations and response contracts.
 *
 * <p>Only categories are read using a discovered board ID. Other lists use their workspace scope.
 */
enum CannyResource {
    BOARDS("boards", "/api/v1/boards/list", "boards", Paging.COMPLETE),
    CATEGORIES("categories", "/api/v1/categories/list", "categories", Paging.OFFSET),
    CHANGELOG_ENTRIES("changelog_entries", "/api/v1/entries/list", "entries", Paging.OFFSET),
    COMMENTS("comments", "/api/v2/comments/list", "items", Paging.CURSOR),
    COMPANIES("companies", "/api/v2/companies/list", "companies", Paging.CURSOR),
    POSTS("posts", "/api/v1/posts/list", "posts", Paging.OFFSET),
    STATUS_CHANGES("status_changes", "/api/v2/status_changes/list", "items", Paging.CURSOR),
    TAGS("tags", "/api/v1/tags/list", "tags", Paging.OFFSET),
    USERS("users", "/api/v2/users/list", "users", Paging.CURSOR),
    VOTES("votes", "/api/v2/votes/list", "items", Paging.CURSOR);

    static final int CHANGELOG_MAX_SKIP = 1000;
    private static final int CHANGELOG_MAX_LIMIT = 10000;

    final String name;
    final String path;
    final String items;
    final Paging paging;

    CannyResource(String name, String path, String items, Paging paging) {
        this.name = name;
        this.path = path;
        this.items = items;
        this.paging = paging;
    }

    int pageLimit(CannyConfig config, int page) {
        if (this == BOARDS) {
            return config.maxItems;
        }
        if (this == CHANGELOG_ENTRIES) {
            // Use the last allowed offset to read up to the full remaining API window.
            return page == 1 ? CHANGELOG_MAX_SKIP : CHANGELOG_MAX_LIMIT;
        }
        return config.pageSize;
    }

    Failure failure(String reason) {
        return new Failure(name, reason);
    }

    /**
     * Distinguishes the complete board response from offset and cursor continuation.
     *
     * <p>Each strategy translates only its native request and response fields.
     */
    enum Paging {
        COMPLETE,
        OFFSET,
        CURSOR
    }

    /**
     * Carries a safe diagnostic for the selected resource.
     *
     * <p>Request bodies, provider payloads and continuation values are excluded.
     */
    static final class Failure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failure(String name, String reason) {
            super("Canny " + name + ": " + reason);
        }
    }
}
