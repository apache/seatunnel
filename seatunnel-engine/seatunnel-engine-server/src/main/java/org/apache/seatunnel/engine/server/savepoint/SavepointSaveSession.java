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

package org.apache.seatunnel.engine.server.savepoint;

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointMeta;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointRequest;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageConstants;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointWriter;
import org.apache.seatunnel.engine.common.env.EnvironmentUtil;

import java.util.Set;
import java.util.UUID;

/**
 * Aggregated write session for one stop-with-savepoint request.
 *
 * <p>Created by the JobMaster before the savepoint is triggered: all pipeline coordinators write
 * their payloads into the same staging attempt, and only after every pipeline reached {@code
 * SUSPEND} the session is committed (publishing {@code _metadata.ser} as the commit marker).
 * Otherwise the staging attempt is aborted.
 */
public class SavepointSaveSession {

    private static final String SAVEPOINT_ID_PREFIX = "savepoint-";
    private static final String SAVEPOINT_ID_SUFFIX = "-";
    private static final String ATTEMPT_ID_PREFIX = "attempt-";

    private final SavepointRequest request;
    private final long triggerTimestamp;
    private final SavepointWriter writer;
    private volatile boolean committed = false;

    public SavepointSaveSession(SavepointStorage storage, long jobId)
            throws CheckpointStorageException {
        this(storage, jobId, null);
    }

    public SavepointSaveSession(
            SavepointStorage storage, long jobId, Set<Integer> expectedPipelineIds)
            throws CheckpointStorageException {
        this.triggerTimestamp = System.currentTimeMillis();
        // Savepoint id must be globally unique: a random suffix guarantees uniqueness even for
        // same-millisecond triggers, the timestamp prefix keeps ordering/debugging readable.
        String savepointId =
                SAVEPOINT_ID_PREFIX
                        + triggerTimestamp
                        + SAVEPOINT_ID_SUFFIX
                        + UUID.randomUUID().toString().substring(0, 8);
        this.request =
                new SavepointRequest(
                        String.valueOf(jobId),
                        savepointId,
                        ATTEMPT_ID_PREFIX + savepointId,
                        expectedPipelineIds);
        this.writer = storage.beginSavepoint(request);
    }

    public SavepointWriter getWriter() {
        return writer;
    }

    public String getSavepointId() {
        return request.getSavepointId();
    }

    /**
     * Publishes the savepoint bundle exactly once (concurrent requests share the session).
     *
     * <p>The storage fills in the manifest entries (file, length, checksum, payload format) from
     * the payloads that were actually written and persists them in {@code _metadata.ser}; the file
     * is the authoritative record, the caller does not need to read the meta back.
     */
    public synchronized void commit() throws CheckpointStorageException {
        if (committed) {
            return;
        }
        SavepointMeta meta =
                new SavepointMeta(
                        SavepointStorageConstants.FORMAT_VERSION,
                        request.getSavepointId(),
                        request.getJobId(),
                        producerVersion(),
                        triggerTimestamp,
                        null,
                        null);
        writer.commitSavepoint(meta);
        committed = true;
    }

    /** Discards the staging attempt; safe on write or commit failures. */
    public synchronized void abort() {
        if (committed) {
            return;
        }
        try {
            writer.abortSavepoint();
        } catch (Exception e) {
            // best effort; the staging directory can also be cleaned manually
            org.slf4j.LoggerFactory.getLogger(SavepointSaveSession.class)
                    .warn(
                            "Failed to abort savepoint staging for job {}, savepoint {}",
                            request.getJobId(),
                            request.getSavepointId(),
                            e);
        }
    }

    private static String producerVersion() {
        try {
            String version = EnvironmentUtil.getVersion().getProjectVersion();
            return version == null ? "unknown" : version;
        } catch (Throwable t) {
            return "unknown";
        }
    }
}
