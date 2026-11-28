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

import java.util.List;

/**
 * Read-only Data Access operations, their parent routes and their provider keys.
 *
 * <p>Optional time fields govern emitted rows and never suppress parent discovery.
 */
enum BugSnagResource {
    ORGANIZATIONS("organizations", "id", null, Scope.ROOT, true, "created_at"),
    PROJECTS("projects", "id", "created_at", Scope.ORGANIZATION, true, "created_at"),
    SAVED_SEARCHES("saved_searches", "id", null, Scope.PROJECT, false, null),
    USAGE_SUMMARY("saved_searches_usage_summary", null, null, Scope.SEARCH, false, null),
    ERRORS("errors", "id", "last_seen", Scope.PROJECT, true, "last_seen"),
    EVENTS("events", "id", "received_at", Scope.PROJECT, true, "timestamp"),
    PIVOTS("pivots", "event_field_display_id", null, Scope.PROJECT, false, null),
    SUPPORTED_INTEGRATIONS("supported_integrations", "key", null, Scope.ROOT, false, null),
    COLLABORATORS("collaborators", "id", null, Scope.ORGANIZATION, true, null),
    TEAMS("teams", "id", null, Scope.ORGANIZATION, true, null),
    EVENT_FIELDS("event_fields", "display_id", null, Scope.PROJECT, false, null),
    RELEASES("releases", "id", "release_time", Scope.PROJECT, true, "timestamp"),
    TRACE_FIELDS("trace_fields", "display_id", null, Scope.PROJECT, false, null);

    final String id;
    final String key;
    final String time;
    final Scope scope;
    final boolean sized;
    final String sort;

    BugSnagResource(String id, String key, String time, Scope scope, boolean sized, String sort) {
        this.id = id;
        this.key = key;
        this.time = time;
        this.scope = scope;
        this.sized = sized;
        this.sort = sort;
    }

    static BugSnagResource from(String text) {
        for (BugSnagResource resource : values()) {
            if (resource.id.equals(text)) {
                return resource;
            }
        }
        throw BugSnagConfig.invalid("resources", "unknown resource name");
    }

    List<String> path(BugSnagWork work) {
        switch (scope) {
            case ORGANIZATION:
                return List.of("organizations", work.organization, id);
            case PROJECT:
                return List.of("projects", work.project, id);
            case SEARCH:
                return List.of("saved_searches", work.search, "usage_summary");
            case ROOT:
                return this == ORGANIZATIONS
                        ? List.of("user", "organizations")
                        : List.of("integrations");
            default:
                throw new IllegalStateException("Invalid BugSnag scope");
        }
    }

    int pageSize(int configured) {
        return this == RELEASES ? Math.min(10, configured) : sized ? configured : 0;
    }

    /**
     * Identifier required to address a resource's API route.
     *
     * <p>Output carries the complete discovered ancestry even when only the last identifier is
     * needed for the network request.
     */
    enum Scope {
        ROOT,
        ORGANIZATION,
        PROJECT,
        SEARCH
    }
}
