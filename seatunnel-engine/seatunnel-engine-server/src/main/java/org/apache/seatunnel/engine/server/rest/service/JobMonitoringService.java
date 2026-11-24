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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.master.JobMonitoringRecord;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads terminal-job monitoring records with bounded sequence-key lookups.
 *
 * <p>Every request reads at most {@code limit} exact IMap keys. Query cost therefore does not grow
 * with the number of retained finished jobs.
 */
public class JobMonitoringService extends BaseService {

    // Initial position that replays from the current retention head.
    private static final String START_BEGINNING = "beginning";

    // Initial position that observes only future ledger commits.
    private static final String START_LATEST = "latest";

    // Cursor marker for an unfiltered terminal-status stream.
    private static final String ALL_STATUSES = "*";

    // Cursor version for sequence-based monitoring.
    private static final String CURSOR_VERSION = "2";

    // Default number of exact sequence keys examined per request.
    private static final int DEFAULT_LIMIT = 100;

    // Hard bound on member work and response record count.
    private static final int MAX_LIMIT = 1000;

    // Hard input bound applied before cursor decoding.
    private static final int MAX_CURSOR_LENGTH = 256;

    // TTL-bound compact monitoring ledger keyed by sequence.
    private final IMap<Long, JobMonitoringRecord> monitoringRecordMap;

    // Committed and retention-head watermarks for the ledger.
    private final IMap<String, Long> monitoringMetadataMap;

    /**
     * Creates a monitoring service backed by the terminal-job sequence ledger.
     *
     * @param nodeEngine local Hazelcast node engine
     */
    public JobMonitoringService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        this.monitoringRecordMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_MONITORING);
        this.monitoringMetadataMap =
                nodeEngine
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_FINISHED_JOB_MONITORING_METADATA);
    }

    /**
     * Returns one bounded sequence window of terminal-job records.
     *
     * @param status optional terminal status filter
     * @param start initial position, either {@code beginning} or {@code latest}
     * @param cursor opaque cursor returned by the previous request
     * @param limit maximum number of sequence slots examined by this request
     * @return monitoring records and the cursor after the examined window
     */
    public JsonObject getFinishedJobChanges(
            String status, String start, String cursor, String limit) {
        int pageSize = parseLimit(limit);
        JobStatus requestedStatus = parseStatus(status);
        long committedSequence = getCommittedSequence();
        long headSequence = getHeadSequence(committedSequence);
        QueryPosition position =
                parsePosition(start, cursor, requestedStatus, committedSequence, headSequence);
        boolean cursorReset = position.sequence < headSequence - 1;
        long effectiveSequence = cursorReset ? headSequence - 1 : position.sequence;

        Set<Long> keys = new HashSet<>();
        long lastSequence = effectiveSequence;
        if (effectiveSequence < committedSequence) {
            lastSequence =
                    Math.min(committedSequence, addWithoutOverflow(effectiveSequence, pageSize));
            for (long sequence = effectiveSequence + 1; sequence <= lastSequence; sequence++) {
                keys.add(sequence);
            }
        }
        Map<Long, JobMonitoringRecord> recordsBySequence =
                keys.isEmpty() ? Collections.emptyMap() : monitoringRecordMap.getAll(keys);
        List<JobMonitoringRecord> records = new ArrayList<>(recordsBySequence.values());
        records.sort((left, right) -> Long.compare(left.getSequence(), right.getSequence()));

        JsonArray data = new JsonArray();
        for (JobMonitoringRecord record : records) {
            if (position.status == null || position.status == record.getJobStatus()) {
                data.add(toMonitoringJson(record));
            }
        }

        boolean hasMore = lastSequence < committedSequence;
        return new JsonObject()
                .add("data", data)
                .add("hasMore", hasMore)
                .add("limit", pageSize)
                .add("scanned", keys.size())
                .add("headSequence", headSequence)
                .add("cursorReset", cursorReset)
                .add("nextCursor", encodeCursor(lastSequence, position.status));
    }

    private long getCommittedSequence() {
        return monitoringMetadataMap.getOrDefault(
                Constant.FINISHED_JOB_MONITORING_COMMITTED_SEQUENCE_KEY, 0L);
    }

    /** Returns a valid retention head clamped to the committed ledger range. */
    private long getHeadSequence(long committedSequence) {
        long headSequence =
                monitoringMetadataMap.getOrDefault(
                        Constant.FINISHED_JOB_MONITORING_HEAD_SEQUENCE_KEY, 1L);
        return Math.max(1L, Math.min(headSequence, addWithoutOverflow(committedSequence, 1)));
    }

    /** Resolves and validates the initial position or opaque continuation cursor. */
    private QueryPosition parsePosition(
            String start,
            String cursor,
            JobStatus requestedStatus,
            long committedSequence,
            long headSequence) {
        if (start != null && cursor != null) {
            throw new IllegalArgumentException("start and cursor must not be provided together.");
        }
        if (cursor != null) {
            QueryPosition position = decodeCursor(cursor);
            if (position.sequence > committedSequence) {
                throw new IllegalArgumentException(
                        "cursor is ahead of the committed monitoring sequence.");
            }
            if (requestedStatus != null && requestedStatus != position.status) {
                throw new IllegalArgumentException("status does not match the cursor.");
            }
            return position;
        }
        if (start == null) {
            throw new IllegalArgumentException("start or cursor must be provided.");
        }
        if (START_BEGINNING.equalsIgnoreCase(start)) {
            return new QueryPosition(headSequence - 1, requestedStatus);
        }
        if (START_LATEST.equalsIgnoreCase(start)) {
            return new QueryPosition(committedSequence, requestedStatus);
        }
        throw new IllegalArgumentException("start must be beginning or latest.");
    }

    /** Parses an optional terminal status filter. */
    private JobStatus parseStatus(String status) {
        if (status == null || status.trim().isEmpty()) {
            return null;
        }
        JobStatus jobStatus;
        try {
            jobStatus = JobStatus.fromString(status.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown job status: " + status, e);
        }
        if (!jobStatus.isEndState()) {
            throw new IllegalArgumentException("status must be a terminal job status.");
        }
        return jobStatus;
    }

    /** Applies the default and hard per-request sequence bound. */
    private int parseLimit(String limit) {
        if (limit == null || limit.trim().isEmpty()) {
            return DEFAULT_LIMIT;
        }
        int parsedLimit;
        try {
            parsedLimit = Integer.parseInt(limit.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("limit must be an integer.", e);
        }
        if (parsedLimit < 1 || parsedLimit > MAX_LIMIT) {
            throw new IllegalArgumentException("limit must be between 1 and " + MAX_LIMIT + ".");
        }
        return parsedLimit;
    }

    /** Adds a page size without allowing sequence overflow. */
    private long addWithoutOverflow(long value, int increment) {
        if (value > Long.MAX_VALUE - increment) {
            return Long.MAX_VALUE;
        }
        return value + increment;
    }

    /** Encodes the scanned sequence and status contract into an opaque cursor. */
    private String encodeCursor(long sequence, JobStatus status) {
        String payload =
                String.join(
                        "|",
                        CURSOR_VERSION,
                        String.valueOf(sequence),
                        status == null ? ALL_STATUSES : status.name());
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    }

    /** Decodes and validates an opaque sequence cursor. */
    private QueryPosition decodeCursor(String cursor) {
        try {
            if (cursor.length() > MAX_CURSOR_LENGTH) {
                throw new IllegalArgumentException("Cursor is too long.");
            }
            String payload =
                    new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8);
            String[] parts = payload.split("\\|", -1);
            if (parts.length != 3 || !CURSOR_VERSION.equals(parts[0])) {
                throw new IllegalArgumentException("Unsupported cursor format.");
            }
            long sequence = Long.parseLong(parts[1]);
            if (sequence < 0) {
                throw new IllegalArgumentException("Cursor sequence must not be negative.");
            }
            JobStatus status = ALL_STATUSES.equals(parts[2]) ? null : parseStatus(parts[2]);
            return new QueryPosition(sequence, status);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid cursor.", e);
        }
    }

    /** Converts one compact ledger record to the public monitoring representation. */
    private JsonObject toMonitoringJson(JobMonitoringRecord record) {
        JsonObject response =
                new JsonObject()
                        .add("sequence", record.getSequence())
                        .add("jobId", String.valueOf(record.getJobId()))
                        .add("jobName", record.getJobName())
                        .add("jobStatus", record.getJobStatus().toString())
                        .add("createTime", record.getSubmitTime())
                        .add("observedTime", record.getObservedTime())
                        .add("errorSummary", record.getErrorSummary());
        if (record.getStartTime() == null) {
            response.add("startTime", Json.NULL);
        } else {
            response.add("startTime", record.getStartTime());
        }
        if (record.getFinishTime() == null) {
            response.add("finishTime", Json.NULL);
        } else {
            response.add("finishTime", record.getFinishTime());
        }
        return response;
    }

    private static final class QueryPosition {
        // Last sequence already examined by the client.
        private final long sequence;

        // Status filter carried by the cursor.
        private final JobStatus status;

        private QueryPosition(long sequence, JobStatus status) {
            this.sequence = sequence;
            this.status = status;
        }
    }
}
