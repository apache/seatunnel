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
package org.apache.seatunnel.connectors.seatunnel.auth0;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Holds the resumable Auth0 resource traversal and its frozen user search limits.
 *
 * <p>The current page's hash verifies replay. Records, access tokens, client secrets and TLS
 * material stay outside the checkpoint.
 */
final class Auth0Split implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private static final long YEAR_IN_MILLIS = TimeUnit.DAYS.toMillis(365);
    private final String scope;
    private final long userEnd;

    // Canonical phases make parent discovery independent from resource selection order.
    Auth0Resource phase;
    Position top = new Position();
    Position members = new Position();
    Position roles = new Position();
    String organizationId;
    String userId;

    // Right-hand ranges are held in order while the active range is split depth first.
    final Deque<Window> windows = new ArrayDeque<>();
    // Budget spans parents and user range subdivisions, including after recovery.
    final Map<Auth0Resource, Integer> requests = new EnumMap<>(Auth0Resource.class);

    Auth0Split(Auth0Config config, long userEnd) {
        this(config.scope, userEnd);
        phase = config.includes(Auth0Resource.CLIENTS) ? Auth0Resource.CLIENTS : null;
        if (config.includes(Auth0Resource.USERS) && config.start.toEpochMilli() <= userEnd) {
            addWindow(config.start.toEpochMilli());
            if (phase == null) {
                phase = Auth0Resource.USERS;
            }
        }
        if (phase == null
                && (config.includes(Auth0Resource.ORGANIZATIONS) || config.needsMembers())) {
            phase = Auth0Resource.ORGANIZATIONS;
        }
    }

    private Auth0Split(String scope, long userEnd) {
        this.scope = scope;
        this.userEnd = userEnd;
    }

    @Override
    public String splitId() {
        return "auth0-management-scan";
    }

    boolean finished() {
        return phase == null;
    }

    /**
     * Detaches mutable counters, windows and opaque cursor guards from the live reader.
     *
     * <p>The returned token can be serialized independently while an HTTPS request is in flight.
     */
    Auth0Split copy() {
        Auth0Split saved = new Auth0Split(scope, userEnd);
        saved.phase = phase;
        saved.top = top.copy();
        saved.members = members.copy();
        saved.roles = roles.copy();
        saved.organizationId = organizationId;
        saved.userId = userId;
        windows.forEach(window -> saved.windows.addLast(window.copy()));
        saved.requests.putAll(requests);
        return saved;
    }

    void validate(Auth0Config config) {
        if (!config.scope.equals(scope)
                || phase != null
                        && phase != Auth0Resource.CLIENTS
                        && phase != Auth0Resource.USERS
                        && phase != Auth0Resource.ORGANIZATIONS
                || phase == Auth0Resource.USERS && windows.isEmpty()
                || phase != Auth0Resource.ORGANIZATIONS && organizationId != null
                || organizationId == null && userId != null
                || organizationId != null && top.digest == null
                || userId != null && members.digest == null
                || !validWindows(config)
                || !top.valid(config.pageSize)
                || !members.valid(config.pageSize)
                || !roles.valid(config.pageSize)) {
            throw new IllegalStateException(
                    "Auth0 checkpoint has incompatible scan options or state");
        }
    }

    private boolean validWindows(Auth0Config config) {
        long previous = Long.MIN_VALUE;
        for (Window window : windows) {
            if (window.first < config.start.toEpochMilli()
                    || window.last > userEnd
                    || window.first > window.last
                    || window.first <= previous
                    || window.count < -1
                    || window.count >= Auth0Api.USER_CAP) {
                return false;
            }
            previous = window.last;
        }
        return true;
    }

    void requireBudget(Auth0Resource resource, int requestMaximum) {
        if (requests.getOrDefault(resource, 0) >= requestMaximum) {
            throw resource.failure("exceeded max_pages_per_resource");
        }
    }

    void recordRequest(Auth0Resource resource) {
        requests.merge(resource, 1, Integer::sum);
    }

    void completePhase(Auth0Config config) {
        top = new Position();
        if (phase == Auth0Resource.CLIENTS && !windows.isEmpty()) {
            phase = Auth0Resource.USERS;
        } else if (phase != Auth0Resource.ORGANIZATIONS
                && (config.includes(Auth0Resource.ORGANIZATIONS) || config.needsMembers())) {
            phase = Auth0Resource.ORGANIZATIONS;
        } else {
            phase = null;
        }
    }

    void finishWindow(Auth0Config config) {
        Window completed = windows.removeFirst();
        top = new Position();
        if (windows.isEmpty()) {
            if (completed.last < userEnd) {
                addWindow(completed.last + 1);
            } else {
                completePhase(config);
            }
        }
    }

    /**
     * Narrows a capped provider query without overlapping any boundary millisecond.
     *
     * <p>Only a window proved to be below the cap may supply user rows.
     */
    void setWindowCount(int count) {
        Window current = windows.getFirst();
        if (count >= Auth0Api.USER_CAP) {
            if (current.first == current.last) {
                throw Auth0Resource.USERS.failure(
                        "user search reaches the 1000-result cap within one millisecond");
            }
            long middle = current.first + (current.last - current.first) / 2;
            windows.removeFirst();
            windows.addFirst(new Window(middle + 1, current.last));
            windows.addFirst(new Window(current.first, middle));
        } else {
            current.count = count;
        }
    }

    private void addWindow(long first) {
        windows.add(new Window(first, Math.min(userEnd, first + YEAR_IN_MILLIS - 1)));
    }

    /**
     * A bounded inclusive interval with a proven count below the provider search cap.
     *
     * <p>A negative count marks an interval that still needs a density probe.
     */
    static final class Window implements Serializable {
        private static final long serialVersionUID = 1L;
        final long first;
        final long last;
        int count = -1;

        Window(long first, long last) {
            this.first = first;
            this.last = last;
        }

        Window copy() {
            Window state = new Window(first, last);
            state.count = count;
            return state;
        }
    }

    /**
     * Tracks one collection while its parent remains current.
     *
     * <p>Only the current provider cursor is retained verbatim. Older cursors and row identities
     * are hashed to detect repeated pagination without storing the response payloads.
     */
    static final class Position implements Serializable {
        private static final long serialVersionUID = 1L;
        int number;
        int row;
        String cursor;
        String digest;
        final Set<String> cursors = new HashSet<>();
        final Set<String> previous = new HashSet<>();
        long previousUpdate = Long.MIN_VALUE;

        void advance(Auth0Resource resource, Auth0Api.Page page) {
            if (page.next != null && !cursors.add(Auth0Config.hash(page.next))) {
                throw resource.failure("pagination repeated a cursor");
            }
            cursor = page.next;
            ++number;
            row = 0;
            digest = null;
            if (!page.rows.isEmpty()) {
                if (resource != Auth0Resource.USERS) {
                    previous.clear();
                } else {
                    previousUpdate = page.rows.get(page.rows.size() - 1).updated;
                }
                page.rows.forEach(value -> previous.add(Auth0Config.hash(value.id)));
            }
        }

        boolean valid(int pageSize) {
            return number >= 0 && row >= 0 && row <= pageSize && (row == 0 || digest != null);
        }

        Position copy() {
            Position state = new Position();
            state.number = number;
            state.row = row;
            state.cursor = cursor;
            state.digest = digest;
            state.cursors.addAll(cursors);
            state.previous.addAll(previous);
            state.previousUpdate = previousUpdate;
            return state;
        }
    }
}
