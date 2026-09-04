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

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.serializer.api.Serializer;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Shared serialization, checksum and naming helpers for savepoint storage implementors. */
public final class SavepointStorageUtils {

    private static final Serializer SERIALIZER = new ProtoStuffSerializer();

    private SavepointStorageUtils() {}

    public static byte[] serializeMeta(SavepointMeta meta) throws CheckpointStorageException {
        try {
            return SERIALIZER.serialize(meta);
        } catch (IOException | RuntimeException e) {
            throw new CheckpointStorageException("Failed to serialize savepoint metadata", e);
        }
    }

    public static SavepointMeta deserializeMeta(byte[] data) throws CheckpointStorageException {
        try {
            return SERIALIZER.deserialize(data, SavepointMeta.class);
        } catch (IOException | RuntimeException e) {
            throw new CheckpointStorageException("Failed to deserialize savepoint metadata", e);
        }
    }

    /** SHA-256 hex digest of the given bytes. */
    public static String sha256Hex(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return hex(digest.digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    /**
     * Canonical manifest checksum: SHA-256 over the entries sorted by pipeline id. The
     * representation must be stable across engine versions (field names/order, one entry per line).
     */
    public static String manifestChecksum(List<SavepointManifestEntry> pipelines) {
        StringBuilder canonical = new StringBuilder();
        List<SavepointManifestEntry> sorted = new ArrayList<>(pipelines);
        sorted.sort(Comparator.comparingInt(SavepointManifestEntry::getPipelineId));
        for (SavepointManifestEntry entry : sorted) {
            canonical
                    .append(entry.getPipelineId())
                    .append('|')
                    .append(entry.getCheckpointId())
                    .append('|')
                    .append(entry.getPayloadFile())
                    .append('|')
                    .append(entry.getPayloadLength())
                    .append('|')
                    .append(entry.getPayloadChecksum())
                    .append('|')
                    .append(entry.getPayloadFormat())
                    .append('\n');
        }
        return sha256Hex(canonical.toString().getBytes(StandardCharsets.UTF_8));
    }

    /** Recomputes and verifies the manifest checksum of a metadata object. */
    public static void verifyManifestChecksum(SavepointMeta meta)
            throws CheckpointStorageException {
        if (meta.getManifestChecksum() == null || meta.getPipelines() == null) {
            throw new CheckpointStorageException(
                    "Savepoint metadata has no manifest or checksum: " + meta.getSavepointId());
        }
        String expected = manifestChecksum(meta.getPipelines());
        if (!expected.equals(meta.getManifestChecksum())) {
            throw new CheckpointStorageException(
                    "Savepoint manifest checksum mismatch for savepoint "
                            + meta.getSavepointId()
                            + ": expected "
                            + meta.getManifestChecksum()
                            + ", computed "
                            + expected);
        }
    }

    public static String pipelinePayloadFileName(int pipelineId, long checkpointId) {
        return pipelineId + "-" + checkpointId + ".ser";
    }

    /**
     * Verifies that the payloads actually written cover exactly the expected pipelines.
     *
     * <p>A savepoint is an all-or-nothing bundle: if the writer's expected pipeline set (declared
     * in {@link SavepointRequest}) is known, commit must not publish a bundle that misses any of
     * them. Duplicate pipeline ids (same pipeline written twice, e.g. with a different checkpoint
     * id) are also rejected because the manifest could not map them unambiguously.
     *
     * @param savepointId savepoint id (for the error message)
     * @param actualPipelineIds pipeline ids that were actually written
     * @param expectedPipelineIds expected pipeline ids (may be null when unknown)
     */
    public static void verifyCompleteBundle(
            String savepointId, Set<Integer> actualPipelineIds, Set<Integer> expectedPipelineIds)
            throws CheckpointStorageException {
        if (expectedPipelineIds == null || expectedPipelineIds.isEmpty()) {
            return;
        }
        Set<Integer> missing = new HashSet<>(expectedPipelineIds);
        missing.removeAll(actualPipelineIds);
        if (!missing.isEmpty()) {
            throw new CheckpointStorageException(
                    "Savepoint bundle "
                            + savepointId
                            + " is incomplete: missing pipeline ids "
                            + missing);
        }
        Set<Integer> unexpected = new HashSet<>(actualPipelineIds);
        unexpected.removeAll(expectedPipelineIds);
        if (!unexpected.isEmpty()) {
            throw new CheckpointStorageException(
                    "Savepoint bundle "
                            + savepointId
                            + " contains unexpected pipeline ids "
                            + unexpected);
        }
        if (actualPipelineIds.size() != expectedPipelineIds.size()) {
            throw new CheckpointStorageException(
                    "Savepoint bundle "
                            + savepointId
                            + " has "
                            + actualPipelineIds.size()
                            + " pipeline payloads but "
                            + expectedPipelineIds.size()
                            + " were expected (duplicate pipeline writes)");
        }
    }

    /** Verifies that the whole bundle is a complete, commit-worthy set. */
    public static void verifyManifestComplete(
            String savepointId, List<SavepointManifestEntry> pipelines)
            throws CheckpointStorageException {
        if (pipelines == null || pipelines.isEmpty()) {
            throw new CheckpointStorageException("Savepoint manifest is empty: " + savepointId);
        }
        Set<Integer> seen = new HashSet<>();
        for (SavepointManifestEntry entry : pipelines) {
            if (!seen.add(entry.getPipelineId())) {
                throw new CheckpointStorageException(
                        "Savepoint manifest of "
                                + savepointId
                                + " contains duplicate pipeline id "
                                + entry.getPipelineId());
            }
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
