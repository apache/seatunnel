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

package org.apache.seatunnel.engine.checkpoint.storage.state;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The on-storage layout of v2 (STIP-45) checkpoints. All paths are relative to the storage
 * namespace and use {@code /} as separator:
 *
 * <pre>
 * &lt;jobId&gt;/&lt;pipelineId&gt;/chk-&lt;checkpointId&gt;/_metadata
 * &lt;jobId&gt;/&lt;pipelineId&gt;/chk-&lt;checkpointId&gt;/subtask-&lt;actionHash&gt;-&lt;index&gt;-&lt;attemptId&gt;.chunk
 * </pre>
 *
 * <p>Every chunk write uses a new attempt id, so a chunk file is written exactly once and a late
 * writer of an earlier attempt can never replace a chunk that a committed {@code _metadata}
 * references. Readers find chunks through the location stored in the state handle.
 *
 * <p>v1 checkpoints are files named {@code <ts>-<rand>-<pipelineId>-<checkpointId>.ser} directly
 * under {@code <jobId>/}; the v2 tree never produces names with the {@code ser} extension, so the
 * two layouts can coexist in the same job directory.
 */
public final class CheckpointFileLayout {

    public static final String SEPARATOR = "/";

    public static final String CHECKPOINT_DIR_PREFIX = "chk-";

    public static final String METADATA_FILE_NAME = "_metadata";

    public static final String CHUNK_FILE_PREFIX = "subtask-";

    public static final String CHUNK_FILE_SUFFIX = ".chunk";

    public static final String TMP_SUFFIX = ".tmp";

    /** File name index used for the coordinator (e.g. split enumerator) state. */
    public static final String COORDINATOR_INDEX_NAME = "coordinator";

    private static final Pattern CHECKPOINT_DIR_PATTERN =
            Pattern.compile("^" + CHECKPOINT_DIR_PREFIX + "([0-9]+)$");

    private static final Pattern ATTEMPT_ID_PATTERN = Pattern.compile("^[0-9a-f]{32}$");

    private static final Pattern CHUNK_FILE_PATTERN =
            Pattern.compile(
                    "^"
                            + CHUNK_FILE_PREFIX
                            + "[0-9a-f]{16}-("
                            + COORDINATOR_INDEX_NAME
                            + "|[0-9]+)-[0-9a-f]{32}"
                            + Pattern.quote(CHUNK_FILE_SUFFIX)
                            + "$");

    private CheckpointFileLayout() {}

    /** Returns {@code <jobId>/<pipelineId>}. */
    public static String pipelineDir(String jobId, String pipelineId) {
        return checkSegment("jobId", jobId) + SEPARATOR + checkSegment("pipelineId", pipelineId);
    }

    /** Returns {@code <jobId>/<pipelineId>/chk-<checkpointId>}. */
    public static String checkpointDir(String jobId, String pipelineId, long checkpointId) {
        if (checkpointId < 0) {
            throw new IllegalArgumentException("checkpointId must be >= 0: " + checkpointId);
        }
        return pipelineDir(jobId, pipelineId) + SEPARATOR + CHECKPOINT_DIR_PREFIX + checkpointId;
    }

    /** Returns the location of the {@code _metadata} file of a checkpoint. */
    public static String metadataPath(String jobId, String pipelineId, long checkpointId) {
        return checkpointDir(jobId, pipelineId, checkpointId) + SEPARATOR + METADATA_FILE_NAME;
    }

    /**
     * Returns the location of a chunk of the given descriptor, written by one attempt.
     *
     * @param attemptId identifies the write attempt, see {@link #newAttemptId()}
     */
    public static String chunkPath(ChunkDescriptor descriptor, String attemptId) {
        if (attemptId == null || !ATTEMPT_ID_PATTERN.matcher(attemptId).matches()) {
            throw new IllegalArgumentException("Invalid chunk attempt id: '" + attemptId + "'");
        }
        String index =
                descriptor.getSubtaskIndex() < 0
                        ? COORDINATOR_INDEX_NAME
                        : String.valueOf(descriptor.getSubtaskIndex());
        return checkpointDir(
                        descriptor.getJobId(),
                        descriptor.getPipelineId(),
                        descriptor.getCheckpointId())
                + SEPARATOR
                + CHUNK_FILE_PREFIX
                + actionHash(descriptor.getActionKey())
                + "-"
                + index
                + "-"
                + attemptId
                + CHUNK_FILE_SUFFIX;
    }

    /** Returns a new, unique chunk attempt id. */
    public static String newAttemptId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * Validates that a chunk location read from a state handle has the engine layout {@code
     * <jobId>/<pipelineId>/chk-<id>/subtask-<hash>-<index>-<attempt>.chunk} and cannot address a
     * file outside the storage namespace (no empty, {@code .}, {@code ..} or backslash segments).
     *
     * @return the location
     * @throws IllegalArgumentException if the location is not a valid chunk location
     */
    public static String checkChunkLocation(String location) {
        if (location == null) {
            throw new IllegalArgumentException("Chunk location must not be null");
        }
        String[] segments = location.split(SEPARATOR, -1);
        if (segments.length != 4) {
            throw new IllegalArgumentException(
                    "Chunk location does not match <jobId>/<pipelineId>/chk-<id>/<chunk>: '"
                            + location
                            + "'");
        }
        checkSegment("jobId", segments[0]);
        checkSegment("pipelineId", segments[1]);
        if (parseCheckpointId(segments[2]) < 0) {
            throw new IllegalArgumentException(
                    "Chunk location has no chk-<id> directory: '" + location + "'");
        }
        if (!CHUNK_FILE_PATTERN.matcher(segments[3]).matches()) {
            throw new IllegalArgumentException(
                    "Chunk location has no engine chunk file name: '" + location + "'");
        }
        return location;
    }

    /**
     * Validates a chunk location like {@link #checkChunkLocation(String)} and that it lies in the
     * directory of the given checkpoint, so a checkpoint can only reference its own chunks.
     *
     * @throws IllegalArgumentException if the location is invalid or belongs to another job,
     *     pipeline or checkpoint
     */
    public static String checkChunkLocation(
            String location, String jobId, String pipelineId, long checkpointId) {
        checkChunkLocation(location);
        String expectedDir = checkpointDir(jobId, pipelineId, checkpointId) + SEPARATOR;
        if (!location.startsWith(expectedDir)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Chunk location '%s' does not belong to checkpoint %d of job %s, "
                                    + "pipeline %s",
                            location, checkpointId, jobId, pipelineId));
        }
        return location;
    }

    /**
     * Parses the checkpoint id of a {@code chk-<id>} directory name.
     *
     * @return the checkpoint id, or {@code -1} when the name is not an engine-generated checkpoint
     *     directory
     */
    public static long parseCheckpointId(String dirName) {
        if (dirName == null) {
            return -1;
        }
        Matcher matcher = CHECKPOINT_DIR_PATTERN.matcher(dirName);
        if (!matcher.matches()) {
            return -1;
        }
        try {
            return Long.parseLong(matcher.group(1));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * Action state key names are free-form (they contain spaces, brackets and connector names), so
     * they are hashed into a fixed-width, file-system-safe token.
     */
    static String actionHash(String actionKey) {
        if (actionKey == null) {
            throw new IllegalArgumentException("actionKey must not be null");
        }
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(actionKey.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(16);
            for (int i = 0; i < 8; i++) {
                hex.append(String.format("%02x", digest[i]));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private static String checkSegment(String name, String value) {
        if (value == null
                || value.isEmpty()
                || value.contains(SEPARATOR)
                || value.contains("\\")
                || value.equals(".")
                || value.equals("..")) {
            throw new IllegalArgumentException(
                    String.format("Invalid checkpoint path segment %s: '%s'", name, value));
        }
        return value;
    }
}
