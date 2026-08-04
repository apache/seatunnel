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

package org.apache.seatunnel.engine.server.task.source;

import org.apache.seatunnel.engine.common.config.server.ManagedSourceRuntimeConfig;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Coordinator-owned ledger that closes the gap between assignment admission and Reader checkpoint
 * inclusion.
 */
public final class SourceAssignmentTracker {
    private static final int SOFT_LIMIT_DENOMINATOR = 5;

    private final int maxEntries;
    private final long maxBytes;
    private final boolean checkpointEnabled;
    private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>();
    private final EnumMap<SourceAssignmentState, Integer> stateCounts =
            new EnumMap<>(SourceAssignmentState.class);
    private long trackedBytes;
    private long compactedEntries;
    private transient Long ownerThreadId;

    public SourceAssignmentTracker(ManagedSourceRuntimeConfig config) {
        this(config.getAssignmentTrackerMaxEntries(), config.getAssignmentTrackerMaxBytes(), true);
    }

    SourceAssignmentTracker(int maxEntries, long maxBytes) {
        this(maxEntries, maxBytes, true);
    }

    SourceAssignmentTracker(int maxEntries, long maxBytes, boolean checkpointEnabled) {
        if (maxEntries <= 0 || maxBytes <= 0) {
            throw new IllegalArgumentException("Source assignment tracker limits must be positive");
        }
        this.maxEntries = maxEntries;
        this.maxBytes = maxBytes;
        this.checkpointEnabled = checkpointEnabled;
    }

    /**
     * Opens ownership for one dispatched assignment chunk in state {@code DISPATCHED}.
     *
     * <p>From here until the entry is released, the engine, not the connector, owns these splits:
     * if the target Reader attempt dies the entry is what allows them to be replayed instead of
     * silently lost. The payloads are retained for exactly that reason, which is why the tracker
     * enforces entry-count and byte limits.
     *
     * @param commandId identity of the assignment command, also the ledger key
     * @param assignmentGroupId groups the chunks of one logical assignment
     * @param senderSequence coordinator-to-Reader channel sequence used for replay fencing
     * @param targetSubtask Reader subtask index the chunk was addressed to
     * @param targetAttemptId Reader attempt the chunk was addressed to
     * @param chunkIndex index of this chunk within its group
     * @param chunkCount total chunks in the group
     * @param splitIds split identifiers carried by this chunk
     * @param splitPayloads serialized splits retained so the assignment can be replayed
     */
    public void recordDispatched(
            String commandId,
            String assignmentGroupId,
            long senderSequence,
            int targetSubtask,
            String targetAttemptId,
            int chunkIndex,
            int chunkCount,
            List<String> splitIds,
            List<byte[]> splitPayloads) {
        checkOwner();
        Entry existing = entries.get(commandId);
        if (existing != null) {
            if (!existing.sameLogicalCommand(
                    assignmentGroupId,
                    senderSequence,
                    targetSubtask,
                    targetAttemptId,
                    chunkIndex,
                    chunkCount,
                    splitIds,
                    splitPayloads)) {
                throw new IllegalStateException(
                        "Assignment command id reused for a different logical assignment");
            }
            return;
        }
        Entry entry =
                new Entry(
                        commandId,
                        assignmentGroupId,
                        senderSequence,
                        targetSubtask,
                        targetAttemptId,
                        chunkIndex,
                        chunkCount,
                        splitIds,
                        splitPayloads,
                        SourceAssignmentState.DISPATCHED,
                        -1L,
                        System.currentTimeMillis());
        long entryBytes = entry.estimatedBytes();
        if (entries.size() >= maxEntries || entryBytes > maxBytes - trackedBytes) {
            throw new IllegalStateException(
                    "Managed Source assignment tracker capacity exhausted: entries="
                            + entries.size()
                            + ", bytes="
                            + trackedBytes);
        }
        entries.put(commandId, entry);
        trackedBytes = Math.addExact(trackedBytes, entryBytes);
        incrementState(entry.state);
    }

    /**
     * Advances an entry to {@code ADMITTED} once the Reader has accepted the command into its
     * mailbox.
     *
     * <p>Admission proves delivery only, not application: the Reader has not yet handed the splits
     * to the connector, so ownership stays with the engine. Transitions are monotonic, so a
     * duplicate or out-of-order admission is ignored rather than moving the entry backwards.
     *
     * @param commandId ledger key of the assignment
     * @param targetAttemptId Reader attempt that admitted it, recorded for replay fencing
     */
    public void markAdmitted(String commandId, String targetAttemptId) {
        checkOwner();
        Entry entry = requireEntry(commandId);
        if (entry.state.isBefore(SourceAssignmentState.ADMITTED)) {
            transition(entry, SourceAssignmentState.ADMITTED);
        }
        entry.targetAttemptId = targetAttemptId;
    }

    /**
     * Advances an entry to {@code APPLIED} once the Reader reports it handed the splits to the
     * connector.
     *
     * <p>The reported split identifiers must match what was dispatched; a mismatch means the two
     * sides disagree about what was assigned and is fatal rather than reconciled. Ownership is
     * still held, because an applied-but-not-yet-checkpointed split must be replayed if the Reader
     * dies.
     *
     * <p>When the job runs without checkpoints there is no later checkpoint to prove inclusion, so
     * the entry is released here instead of accumulating for the lifetime of the job.
     *
     * @param commandId ledger key of the assignment
     * @param targetAttemptId Reader attempt reporting application; a stale attempt is ignored
     * @param appliedSplitIds split identifiers the Reader claims it applied
     */
    public void markApplied(
            String commandId, String targetAttemptId, List<String> appliedSplitIds) {
        checkOwner();
        Entry entry = requireEntry(commandId);
        if (!entry.targetAttemptId.equals(targetAttemptId)) {
            return;
        }
        if (!entry.splitIds.equals(appliedSplitIds)) {
            throw new IllegalStateException(
                    "Reader applied proof does not match tracked Source assignment " + commandId);
        }
        if (entry.state.isBefore(SourceAssignmentState.APPLIED)) {
            transition(entry, SourceAssignmentState.APPLIED);
        }
        if (!checkpointEnabled) {
            entries.remove(commandId);
            removeAccounting(entry);
        }
    }

    /** Rebinds an uncheckpointed assignment to a new Reader attempt and channel sequence. */
    public void rebindForReplay(String commandId, String targetAttemptId, long senderSequence) {
        checkOwner();
        Entry entry = requireEntry(commandId);
        long previousBytes = entry.estimatedBytes();
        entry.targetAttemptId = targetAttemptId;
        entry.senderSequence = senderSequence;
        transition(entry, SourceAssignmentState.DISPATCHED);
        entry.includedCheckpointId = -1L;
        trackedBytes += entry.estimatedBytes() - previousBytes;
        if (trackedBytes > maxBytes) {
            throw new IllegalStateException(
                    "Rebound Source assignment tracker exceeds configured byte limit");
        }
    }

    public boolean contains(String commandId) {
        checkOwner();
        return entries.containsKey(commandId);
    }

    /**
     * Advances entries to {@code CHECKPOINT_INCLUDED} using a Reader checkpoint ownership proof.
     *
     * <p>This is the transfer point of the whole protocol: once a split is durably part of the
     * Reader's checkpoint state, restoring that checkpoint restores the split, so the engine no
     * longer has to replay it. Entries are only advanced when the proof comes from the attempt that
     * currently owns them and covers the identifiers they hold.
     *
     * @param subtask Reader subtask the proof came from
     * @param readerAttemptId Reader attempt the proof came from; stale attempts are ignored
     * @param checkpointId checkpoint the splits were included in
     * @param appliedWatermark contiguous command watermark the Reader had applied
     * @param checkpointSplitIds split identifiers the Reader recorded in that checkpoint
     */
    public void markReaderCheckpointIncluded(
            int subtask,
            String readerAttemptId,
            long checkpointId,
            long appliedWatermark,
            Set<String> checkpointSplitIds) {
        checkOwner();
        for (Entry entry : entries.values()) {
            if (entry.targetSubtask != subtask
                    || !entry.targetAttemptId.equals(readerAttemptId)
                    || entry.senderSequence > appliedWatermark
                    || !checkpointSplitIds.containsAll(entry.splitIds)) {
                continue;
            }
            transition(entry, SourceAssignmentState.CHECKPOINT_INCLUDED);
            entry.includedCheckpointId = checkpointId;
        }
    }

    /**
     * Removes assignments whose split ownership is proven by a completed Reader checkpoint.
     *
     * <p>Reconciliation is intentionally independent of the old target subtask because rescale may
     * redistribute one old Reader state to a different new subtask.
     */
    public void markRestoredSplitsIncluded(Set<String> restoredSplitIds) {
        checkOwner();
        Iterator<Map.Entry<String, Entry>> iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry entry = iterator.next().getValue();
            if (restoredSplitIds.containsAll(entry.splitIds)) {
                iterator.remove();
                removeAccounting(entry);
            }
        }
    }

    /**
     * Releases ownership of every entry proven to be in a checkpoint at or below {@code
     * checkpointId}.
     *
     * <p>Releasing before the checkpoint completes would lose splits if that checkpoint were later
     * aborted; never releasing would grow the ledger for the lifetime of the job. Completion is the
     * first point where both risks are gone, so it is where the retained split payloads are
     * dropped.
     *
     * @param checkpointId checkpoint the master reported complete
     */
    public void checkpointCompleted(long checkpointId) {
        checkOwner();
        Iterator<Map.Entry<String, Entry>> iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry entry = iterator.next().getValue();
            if (entry.state == SourceAssignmentState.CHECKPOINT_INCLUDED
                    && entry.includedCheckpointId >= 0
                    && entry.includedCheckpointId <= checkpointId) {
                transition(entry, SourceAssignmentState.GC_ELIGIBLE);
                iterator.remove();
                removeAccounting(entry);
            }
        }
    }

    public List<Entry> assignmentsForReader(int subtask) {
        checkOwner();
        return entries.values().stream()
                .filter(entry -> entry.targetSubtask == subtask)
                .filter(
                        entry ->
                                entry.state != SourceAssignmentState.CHECKPOINT_INCLUDED
                                        && entry.state != SourceAssignmentState.GC_ELIGIBLE)
                .map(Entry::copy)
                .collect(Collectors.toList());
    }

    /**
     * Removes and returns assignments targeting subtasks that no longer exist after rescale.
     *
     * <p>The caller returns their connector splits to the enumerator before any Reader is marked
     * ready.
     */
    public List<Entry> takeAssignmentsForMissingReaders(Set<Integer> activeSubtasks) {
        checkOwner();
        List<Entry> orphaned = new ArrayList<>();
        Iterator<Map.Entry<String, Entry>> iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry entry = iterator.next().getValue();
            if (!activeSubtasks.contains(entry.targetSubtask)) {
                orphaned.add(entry.copy());
                iterator.remove();
                removeAccounting(entry);
            }
        }
        return orphaned;
    }

    public Collection<Entry> entries() {
        checkOwner();
        return Collections.unmodifiableList(
                entries.values().stream().map(Entry::copy).collect(Collectors.toList()));
    }

    /**
     * Rebuilds the ledger from checkpointed entries during coordinator restore.
     *
     * <p>Replaces the current contents outright rather than merging: after a restore the checkpoint
     * is the only trustworthy record of who owned what, and merging live entries into it would
     * resurrect assignments the checkpoint deliberately does not contain. Entries are copied so the
     * caller cannot mutate ledger state afterwards, and the configured limits are re-applied to the
     * restored set.
     *
     * @param restoredEntries entries decoded from the coordinator checkpoint
     */
    public void restore(Collection<Entry> restoredEntries) {
        checkOwner();
        entries.clear();
        stateCounts.clear();
        trackedBytes = 0L;
        for (Entry entry : restoredEntries) {
            Entry copy = entry.copy();
            if (entries.put(copy.commandId, copy) != null) {
                throw new IllegalStateException(
                        "Duplicate assignment command in coordinator checkpoint: "
                                + copy.commandId);
            }
            trackedBytes = Math.addExact(trackedBytes, copy.estimatedBytes());
            incrementState(copy.state);
        }
        if (entries.size() > maxEntries || trackedBytes > maxBytes) {
            throw new IllegalStateException(
                    "Restored assignment tracker exceeds configured production limits");
        }
    }

    public int size() {
        checkOwner();
        return entries.size();
    }

    public long trackedBytes() {
        checkOwner();
        return trackedBytes;
    }

    /** Returns the current number of ledger entries in one bounded protocol state. */
    public int stateCount(SourceAssignmentState state) {
        checkOwner();
        return stateCounts.getOrDefault(state, 0);
    }

    /** Returns the cumulative number of entries reconciled out of this runtime's ledger. */
    public long compactedEntries() {
        checkOwner();
        return compactedEntries;
    }

    /**
     * Returns whether the tracker crossed its soft watermark and should stop requesting new work.
     */
    public boolean isNearCapacity() {
        checkOwner();
        long softEntryLimit = maxEntries - Math.max(1L, maxEntries / SOFT_LIMIT_DENOMINATOR);
        long softByteLimit = maxBytes - Math.max(1L, maxBytes / SOFT_LIMIT_DENOMINATOR);
        return entries.size() >= softEntryLimit || trackedBytes >= softByteLimit;
    }

    /** Returns the age of the oldest uncheckpointed assignment using wall-clock time. */
    public long oldestAssignmentAgeMillis(long nowEpochMillis) {
        checkOwner();
        long oldest =
                entries.values().stream()
                        .mapToLong(entry -> entry.createdEpochMillis)
                        .min()
                        .orElse(nowEpochMillis);
        return Math.max(0L, nowEpochMillis - oldest);
    }

    private Entry requireEntry(String commandId) {
        Entry entry = entries.get(commandId);
        if (entry == null) {
            throw new IllegalStateException("Unknown Source assignment command " + commandId);
        }
        return entry;
    }

    private void transition(Entry entry, SourceAssignmentState newState) {
        if (entry.state == newState) {
            return;
        }
        decrementState(entry.state);
        entry.state = newState;
        incrementState(newState);
    }

    private void removeAccounting(Entry entry) {
        trackedBytes -= entry.estimatedBytes();
        decrementState(entry.state);
        compactedEntries++;
        if (trackedBytes < 0) {
            throw new IllegalStateException(
                    "Managed Source assignment tracker byte accounting underflow");
        }
    }

    private void incrementState(SourceAssignmentState state) {
        stateCounts.merge(state, 1, Integer::sum);
    }

    private void decrementState(SourceAssignmentState state) {
        int current = stateCounts.getOrDefault(state, 0);
        if (current <= 0) {
            throw new IllegalStateException(
                    "Managed Source assignment tracker state accounting underflow");
        }
        if (current == 1) {
            stateCounts.remove(state);
        } else {
            stateCounts.put(state, current - 1);
        }
    }

    private void checkOwner() {
        long current = Thread.currentThread().getId();
        if (ownerThreadId == null) {
            ownerThreadId = current;
        } else if (ownerThreadId != current) {
            throw new IllegalStateException(
                    "SourceAssignmentTracker accessed outside its coordinator event loop");
        }
    }

    /** Immutable-on-copy assignment entry persisted with the coordinator checkpoint. */
    public static final class Entry {
        private final String commandId;
        private final String assignmentGroupId;
        private long senderSequence;
        private final int targetSubtask;
        private String targetAttemptId;
        private final int chunkIndex;
        private final int chunkCount;
        private final List<String> splitIds;
        private final List<byte[]> splitPayloads;
        private SourceAssignmentState state;
        private long includedCheckpointId;
        private final long createdEpochMillis;

        Entry(
                String commandId,
                String assignmentGroupId,
                long senderSequence,
                int targetSubtask,
                String targetAttemptId,
                int chunkIndex,
                int chunkCount,
                List<String> splitIds,
                List<byte[]> splitPayloads,
                SourceAssignmentState state,
                long includedCheckpointId,
                long createdEpochMillis) {
            if (splitIds == null || splitPayloads == null) {
                throw new IllegalArgumentException(
                        "Assignment split identifiers and payloads must not be null");
            }
            if (splitPayloads.stream().anyMatch(payload -> payload == null)) {
                throw new IllegalArgumentException("Assignment split payload must not be null");
            }
            this.commandId = commandId;
            this.assignmentGroupId = assignmentGroupId;
            this.senderSequence = senderSequence;
            this.targetSubtask = targetSubtask;
            this.targetAttemptId = targetAttemptId;
            this.chunkIndex = chunkIndex;
            this.chunkCount = chunkCount;
            this.splitIds = Collections.unmodifiableList(new ArrayList<>(splitIds));
            List<byte[]> copiedPayloads = new ArrayList<>(splitPayloads.size());
            for (byte[] payload : splitPayloads) {
                copiedPayloads.add(payload.clone());
            }
            this.splitPayloads = Collections.unmodifiableList(copiedPayloads);
            this.state = state;
            this.includedCheckpointId = includedCheckpointId;
            this.createdEpochMillis = createdEpochMillis;
            validate();
        }

        private boolean sameLogicalCommand(
                String group,
                long sequence,
                int subtask,
                String currentTargetAttemptId,
                int currentChunkIndex,
                int currentChunkCount,
                List<String> currentSplitIds,
                List<byte[]> currentSplitPayloads) {
            return assignmentGroupId.equals(group)
                    && senderSequence == sequence
                    && targetSubtask == subtask
                    && targetAttemptId.equals(currentTargetAttemptId)
                    && chunkIndex == currentChunkIndex
                    && chunkCount == currentChunkCount
                    && splitIds.equals(currentSplitIds)
                    && payloadsEqual(splitPayloads, currentSplitPayloads);
        }

        long estimatedBytes() {
            long bytes =
                    192L
                            + utf8Length(commandId)
                            + utf8Length(assignmentGroupId)
                            + utf8Length(targetAttemptId);
            for (String splitId : splitIds) {
                bytes = Math.addExact(bytes, utf8Length(splitId));
            }
            for (byte[] payload : splitPayloads) {
                bytes = Math.addExact(bytes, payload.length);
            }
            return bytes;
        }

        private void validate() {
            requireIdentifier(commandId, "commandId");
            requireIdentifier(assignmentGroupId, "assignmentGroupId");
            requireIdentifier(targetAttemptId, "targetAttemptId");
            if (senderSequence <= 0 || targetSubtask < 0) {
                throw new IllegalArgumentException(
                        "Assignment sequence and target subtask are invalid");
            }
            if (chunkCount <= 0
                    || chunkCount > SourceCommandEnvelope.MAX_CHUNK_COUNT
                    || chunkIndex < 0
                    || chunkIndex >= chunkCount) {
                throw new IllegalArgumentException("Assignment chunk metadata is invalid");
            }
            if (splitIds.isEmpty() || splitIds.size() != splitPayloads.size()) {
                throw new IllegalArgumentException(
                        "Assignment split identifiers and payloads must be non-empty and aligned");
            }
            if (splitIds.stream().distinct().count() != splitIds.size()) {
                throw new IllegalArgumentException(
                        "Assignment split identifiers must be unique within a command");
            }
            if (state == null || createdEpochMillis < 0) {
                throw new IllegalArgumentException("Assignment state metadata is invalid");
            }
            if ((state == SourceAssignmentState.CHECKPOINT_INCLUDED)
                    != (includedCheckpointId >= 0)) {
                throw new IllegalArgumentException(
                        "Assignment checkpoint inclusion metadata is inconsistent");
            }
            for (String splitId : splitIds) {
                if (splitId == null || splitId.trim().isEmpty()) {
                    throw new IllegalArgumentException(
                            "Assignment split identifier must not be blank");
                }
            }
        }

        private static boolean payloadsEqual(List<byte[]> left, List<byte[]> right) {
            if (left.size() != right.size()) {
                return false;
            }
            for (int i = 0; i < left.size(); i++) {
                if (!Arrays.equals(left.get(i), right.get(i))) {
                    return false;
                }
            }
            return true;
        }

        private static int utf8Length(String value) {
            return value.getBytes(StandardCharsets.UTF_8).length;
        }

        private static void requireIdentifier(String value, String field) {
            if (value == null
                    || value.trim().isEmpty()
                    || value.length() > SourceCommandEnvelope.MAX_IDENTIFIER_LENGTH) {
                throw new IllegalArgumentException(
                        "Assignment " + field + " is blank or exceeds the wire limit");
            }
        }

        Entry copy() {
            return new Entry(
                    commandId,
                    assignmentGroupId,
                    senderSequence,
                    targetSubtask,
                    targetAttemptId,
                    chunkIndex,
                    chunkCount,
                    splitIds,
                    splitPayloads,
                    state,
                    includedCheckpointId,
                    createdEpochMillis);
        }

        public String getCommandId() {
            return commandId;
        }

        public String getAssignmentGroupId() {
            return assignmentGroupId;
        }

        public long getSenderSequence() {
            return senderSequence;
        }

        public int getTargetSubtask() {
            return targetSubtask;
        }

        public String getTargetAttemptId() {
            return targetAttemptId;
        }

        public int getChunkIndex() {
            return chunkIndex;
        }

        public int getChunkCount() {
            return chunkCount;
        }

        public List<String> getSplitIds() {
            return splitIds;
        }

        public List<byte[]> getSplitPayloads() {
            List<byte[]> copiedPayloads = new ArrayList<>(splitPayloads.size());
            for (byte[] payload : splitPayloads) {
                copiedPayloads.add(payload.clone());
            }
            return copiedPayloads;
        }

        public SourceAssignmentState getState() {
            return state;
        }

        public long getIncludedCheckpointId() {
            return includedCheckpointId;
        }

        public long getCreatedEpochMillis() {
            return createdEpochMillis;
        }
    }
}
