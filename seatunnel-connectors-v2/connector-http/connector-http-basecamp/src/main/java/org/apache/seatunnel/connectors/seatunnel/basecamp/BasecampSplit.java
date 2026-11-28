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
package org.apache.seatunnel.connectors.seatunnel.basecamp;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Resumable account scan with fixed recording time bounds and two levels of parent position.
 *
 * <p>Only the validated Link query, fingerprints and accepted offsets are saved, along with the
 * current provider IDs. Records and authentication material stay outside the split.
 */
final class BasecampSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private final String scope;
    final Instant start;
    final Instant end;

    // Projects includes any requested schedules and schedule entries even without project output.
    BasecampResource phase;
    Position top = new Position();
    Position entries = new Position();
    String projectId;
    int dockRow;
    String dockDigest;
    String scheduleId;

    BasecampSplit(BasecampConfig config, Instant cutoff) {
        this(config.scope, config.start, cutoff);
        phase = config.needsProjects() ? BasecampResource.PROJECTS : nextFeed(config, null);
    }

    private BasecampSplit(String scope, Instant start, Instant end) {
        this.scope = scope;
        this.start = start;
        this.end = end;
    }

    @Override
    public String splitId() {
        return "basecamp-account-scan";
    }

    boolean finished() {
        return phase == null;
    }

    /**
     * Exposes a detached cursor and parent frame for coordinator and reader checkpoints.
     *
     * <p>A restored child remains under the same verified project and schedule.
     */
    BasecampSplit copy() {
        BasecampSplit saved = new BasecampSplit(scope, start, end);
        saved.phase = phase;
        saved.top = top.copy();
        saved.entries = entries.copy();
        saved.projectId = projectId;
        saved.dockRow = dockRow;
        saved.dockDigest = dockDigest;
        saved.scheduleId = scheduleId;
        return saved;
    }

    void validate(BasecampConfig config) {
        if (!config.scope.equals(scope)
                || end == null
                || phase != null
                        && phase != BasecampResource.PROJECTS
                        && phase.recordingType == null
                || phase != null
                        && phase.recordingType != null
                        && (start == null || start.isAfter(end))
                || projectId != null
                        && (phase != BasecampResource.PROJECTS
                                || !BasecampResource.validId(projectId)
                                || top.digest == null)
                || scheduleId != null
                        && (projectId == null
                                || !BasecampResource.validId(scheduleId)
                                || dockDigest == null)
                || dockRow < 0
                || dockRow > config.maxPageItems
                || !top.valid(config.maxPageItems)
                || !entries.valid(config.maxPageItems)) {
            throw new IllegalStateException(
                    "Basecamp checkpoint has incompatible scan options or state");
        }
    }

    void completePhase(BasecampConfig config) {
        phase = nextFeed(config, phase);
        top = new Position();
    }

    void completeProject() {
        ++top.row;
        projectId = null;
        dockRow = 0;
        dockDigest = null;
    }

    void completeSchedule() {
        ++dockRow;
        scheduleId = null;
        entries = new Position();
    }

    private BasecampResource nextFeed(BasecampConfig config, BasecampResource after) {
        if (start == null || start.isAfter(end)) {
            return null;
        }
        if ((after == null || after == BasecampResource.PROJECTS)
                && config.includes(BasecampResource.TODOS)) {
            return BasecampResource.TODOS;
        }
        if (after != BasecampResource.MESSAGES && config.includes(BasecampResource.MESSAGES)) {
            return BasecampResource.MESSAGES;
        }
        return null;
    }

    /**
     * Tracks a projects, recording, or schedule entries collection at one exact query.
     *
     * <p>The continuation history prevents cycles. The previous page identity and update time
     * provide a bounded check against overlapping or misordered pages.
     */
    static final class Position implements Serializable {
        private static final long serialVersionUID = 1L;
        int page;
        int row;
        String query;
        String digest;
        Instant previousUpdated;
        Set<String> previousIds = Set.of();
        private final Set<String> seen = new HashSet<>();

        void advance(BasecampResource resource, BasecampApi.Page current, int maximum) {
            if (page + 1 >= maximum) {
                throw resource.failure("exceeded max_pages_per_collection");
            }
            if (!seen.add(BasecampConfig.hash(current.next))) {
                throw resource.failure("repeated continuation link");
            }
            query = current.next;
            ++page;
            row = 0;
            digest = null;
            previousUpdated = current.tail;
            if (!current.rows.isEmpty()) {
                previousIds =
                        current.rows.stream()
                                .map(value -> BasecampConfig.hash(value.id))
                                .collect(Collectors.toUnmodifiableSet());
            }
        }

        boolean valid(int maximum) {
            return page >= 0
                    && row >= 0
                    && row <= maximum
                    && (row == 0 || digest != null)
                    && (page == 0 && query == null || page > 0 && query != null);
        }

        Position copy() {
            Position saved = new Position();
            saved.page = page;
            saved.row = row;
            saved.query = query;
            saved.digest = digest;
            saved.previousUpdated = previousUpdated;
            saved.previousIds = previousIds;
            saved.seen.addAll(seen);
            return saved;
        }
    }
}
