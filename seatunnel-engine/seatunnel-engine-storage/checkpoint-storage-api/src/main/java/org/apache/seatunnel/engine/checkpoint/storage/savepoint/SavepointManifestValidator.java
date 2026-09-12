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

package org.apache.seatunnel.engine.checkpoint.storage.savepoint;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Shared validation of a completed savepoint bundle (metadata + manifest + payloads).
 *
 * <p>Called by every storage implementation on read so that a tampered or corrupted bundle fails
 * with a precise, actionable error instead of being silently accepted. Validation covers:
 *
 * <ul>
 *   <li>metadata identity: job id and savepoint id must match the requested ones
 *   <li>format version within the supported window (single source of truth: {@link
 *       SavepointStorageConstants#FORMAT_VERSION})
 *   <li>manifest: no duplicate pipeline ids, no unexpected payload formats, safe relative file
 *       names (no absolute paths, no {@code ..}, no path separators)
 *   <li>payload consistency: the deserialized {@link PipelineState} ids must equal the manifest
 *       entry ids for the same pipeline
 * </ul>
 */
public final class SavepointManifestValidator {

    private SavepointManifestValidator() {}

    /**
     * Metadata-level validation that can run before any payload byte is read: identity (job /
     * savepoint id), format version window, manifest well-formedness (no empty/duplicate
     * pipelines), supported payload format and safe file names.
     *
     * @param meta bundle metadata read from {@code _metadata.ser}
     * @param expectedJobId job id the bundle is being read for
     * @param expectedSavepointId savepoint id the bundle is being read for
     * @throws CheckpointStorageException if any consistency check fails
     */
    public static void validateMetadata(
            SavepointMeta meta, String expectedJobId, String expectedSavepointId)
            throws CheckpointStorageException {
        String savepointId = meta.getSavepointId();
        if (expectedJobId != null && !expectedJobId.equals(meta.getJobId())) {
            throw new CheckpointStorageException(
                    "Savepoint "
                            + savepointId
                            + " metadata job id mismatch: expected "
                            + expectedJobId
                            + ", got "
                            + meta.getJobId());
        }
        if (expectedSavepointId != null && !expectedSavepointId.equals(savepointId)) {
            throw new CheckpointStorageException(
                    "Savepoint metadata id mismatch: expected "
                            + expectedSavepointId
                            + ", got "
                            + savepointId);
        }
        if (meta.getFormatVersion() < 1
                || meta.getFormatVersion() > SavepointStorageConstants.FORMAT_VERSION) {
            throw new CheckpointStorageException(
                    "Savepoint "
                            + savepointId
                            + " has unsupported format version "
                            + meta.getFormatVersion()
                            + " (supported: 1.."
                            + SavepointStorageConstants.FORMAT_VERSION
                            + ")");
        }
        if (meta.getPipelines() == null || meta.getPipelines().isEmpty()) {
            throw new CheckpointStorageException("Savepoint " + savepointId + " has no pipelines");
        }

        Set<Integer> seen = new HashSet<>();
        for (SavepointManifestEntry entry : meta.getPipelines()) {
            if (!seen.add(entry.getPipelineId())) {
                throw new CheckpointStorageException(
                        "Savepoint "
                                + savepointId
                                + " manifest contains duplicate pipeline id "
                                + entry.getPipelineId());
            }
            if (!SavepointStorageConstants.PAYLOAD_FORMAT_V1.equals(entry.getPayloadFormat())) {
                throw new CheckpointStorageException(
                        "Savepoint "
                                + savepointId
                                + " manifest entry for pipeline "
                                + entry.getPipelineId()
                                + " has unsupported payload format "
                                + entry.getPayloadFormat());
            }
            validatePayloadFileName(savepointId, entry);
        }
    }

    /**
     * Full bundle validation: metadata checks plus payload-consistency checks. Must be invoked
     * after the payload bytes have been read and deserialized into {@link PipelineState}.
     *
     * @param meta bundle metadata read from {@code _metadata.ser}
     * @param expectedJobId job id the bundle is being read for
     * @param expectedSavepointId savepoint id the bundle is being read for
     * @param pipelineStates deserialized payloads keyed by pipeline id
     * @throws CheckpointStorageException if any consistency check fails
     */
    public static void validate(
            SavepointMeta meta,
            String expectedJobId,
            String expectedSavepointId,
            Map<Integer, PipelineState> pipelineStates)
            throws CheckpointStorageException {
        validateMetadata(meta, expectedJobId, expectedSavepointId);
        String savepointId = meta.getSavepointId();

        Set<Integer> seen = new HashSet<>();
        for (SavepointManifestEntry entry : meta.getPipelines()) {
            seen.add(entry.getPipelineId());
            PipelineState state = pipelineStates.get(entry.getPipelineId());
            if (state == null) {
                throw new CheckpointStorageException(
                        "Savepoint "
                                + savepointId
                                + " payload missing for pipeline "
                                + entry.getPipelineId());
            }
            if (state.getPipelineId() != entry.getPipelineId()) {
                throw new CheckpointStorageException(
                        "Savepoint "
                                + savepointId
                                + " payload pipeline id mismatch for pipeline "
                                + entry.getPipelineId()
                                + ": got "
                                + state.getPipelineId());
            }
            if (state.getCheckpointId() != entry.getCheckpointId()) {
                throw new CheckpointStorageException(
                        "Savepoint "
                                + savepointId
                                + " payload checkpoint id mismatch for pipeline "
                                + entry.getPipelineId()
                                + ": expected "
                                + entry.getCheckpointId()
                                + ", got "
                                + state.getCheckpointId());
            }
            if (!meta.getJobId().equals(state.getJobId())) {
                throw new CheckpointStorageException(
                        "Savepoint "
                                + savepointId
                                + " payload job id mismatch for pipeline "
                                + entry.getPipelineId()
                                + ": expected "
                                + meta.getJobId()
                                + ", got "
                                + state.getJobId());
            }
        }
        if (pipelineStates.size() != seen.size()) {
            throw new CheckpointStorageException(
                    "Savepoint "
                            + savepointId
                            + " payload count "
                            + pipelineStates.size()
                            + " does not match manifest "
                            + seen.size());
        }
    }

    /**
     * Payload file names come from the manifest and must stay inside the bundle directory: reject
     * absolute paths, path separators, {@code .}, {@code ..} and hidden names.
     */
    private static void validatePayloadFileName(String savepointId, SavepointManifestEntry entry)
            throws CheckpointStorageException {
        String name = entry.getPayloadFile();
        if (name == null
                || name.isEmpty()
                || name.startsWith("/")
                || name.startsWith("\\")
                || name.contains("/")
                || name.contains("\\")
                || name.equals(".")
                || name.equals("..")
                || name.startsWith(".")) {
            throw new CheckpointStorageException(
                    "Savepoint "
                            + savepointId
                            + " manifest entry for pipeline "
                            + entry.getPipelineId()
                            + " has unsafe payload file name '"
                            + name
                            + "'");
        }
    }
}
