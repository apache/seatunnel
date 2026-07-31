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

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.scheduler.AsyncTaskKey;
import org.apache.seatunnel.api.source.scheduler.Cancellable;
import org.apache.seatunnel.engine.common.config.server.ManagedSourceRuntimeConfig;
import org.apache.seatunnel.engine.common.runtime.source.ManagedSourceRuntimeSelection;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;
import org.apache.seatunnel.engine.server.task.operation.checkpoint.BarrierFlowOperation;
import org.apache.seatunnel.engine.server.task.operation.source.AssignSplitOperation;
import org.apache.seatunnel.engine.server.task.operation.source.ManagedSourceCommandOperation;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Single-owner Source coordinator runtime used only by the managed lane.
 *
 * <p>Hazelcast operation and worker threads may call admission methods, but only {@link
 * #runOneTurn()} invokes the connector enumerator or mutates checkpoint-visible runtime state.
 */
public final class ManagedSourceCoordinatorRuntime<SplitT extends SourceSplit>
        implements AutoCloseable {
    private static final int MAX_CHECKPOINT_HISTORY = 128;

    private final SourceSplitEnumeratorTask<SplitT> task;
    private final Serializer<SplitT> splitSerializer;
    private final Serializer<Serializable> enumeratorStateSerializer;
    private final ManagedSourceRuntimeConfig config;
    private final ManagedSourceRuntimeSelection selection;
    private final String coordinatorAttemptId = UUID.randomUUID().toString();
    private final String coordinatorEpoch = UUID.randomUUID().toString();
    private final SourceAssignmentTracker assignmentTracker;
    private final ManagedCoordinatorScheduler scheduler;
    private final ManagedSourceMemoryBudget outboundMemoryBudget;
    private final ManagedSourceRuntimeMetrics metrics;
    private final Queue<ControlEvent> controlEvents = new ArrayDeque<>();
    private final Map<Integer, ReaderRegistration> readers = new LinkedHashMap<>();
    private final Map<String, ReaderCommandMailbox> inboundMailboxes = new LinkedHashMap<>();
    private final Map<Integer, RegistrationFence> registrationFences = new HashMap<>();
    private final Map<Integer, Cancellable> registrationDeadlineTimers = new HashMap<>();
    private final Map<Integer, Long> nextReaderCommandSequences = new HashMap<>();
    private final Map<String, Integer> transportRetries = new HashMap<>();
    private final Map<String, Long> transportFirstAttemptNanos = new HashMap<>();
    private final Map<String, CheckpointReportAccumulator> checkpointReports = new HashMap<>();
    private final Map<String, SplitIdChunkAccumulator> restoredSplitReports = new HashMap<>();
    private final Set<String> pendingApplicationAcks = new HashSet<>();
    private final Set<Integer> noMoreSplitsSubtasks = new HashSet<>();
    private final Set<Integer> deferredSplitRequests = new HashSet<>();
    private final Set<Long> completedCheckpoints = new HashSet<>();
    private final Set<Long> abortedCheckpoints = new HashSet<>();
    private final AtomicReference<Throwable> asynchronousFailure = new AtomicReference<>();
    private final AtomicBoolean schedulerDrainAdmitted = new AtomicBoolean();
    private final Object admissionLock = new Object();

    private SourceSplitEnumerator<SplitT, Serializable> enumerator;
    private Long ownerThreadId;
    private int normalControlEvents;
    private long inboundMailboxBytes;
    private long inboundReservedBytes;
    private int inboundMailboxCommands;
    private int inboundReservedCommands;
    private int outboundCommands;
    private long outboundBytes;
    private long cachedOldestInboundAgeNanos;
    private long lastOldestInboundSampleNanos;
    private long assignmentBackpressureStartNanos;
    private boolean allReadersNoMoreSplits;
    private long nextNoMoreSplitsGeneration;
    private boolean closed;

    public ManagedSourceCoordinatorRuntime(
            SourceSplitEnumeratorTask<SplitT> task,
            Serializer<SplitT> splitSerializer,
            Serializer<Serializable> enumeratorStateSerializer,
            ManagedSourceRuntimeConfig config,
            ManagedSourceRuntimeSelection selection,
            ClassLoader connectorClassLoader) {
        this.task = task;
        this.splitSerializer = splitSerializer;
        this.enumeratorStateSerializer = enumeratorStateSerializer;
        this.config = config;
        this.selection = selection;
        this.metrics =
                new ManagedSourceRuntimeMetrics(
                        task.getMetricsContext(),
                        task.getSourceAction().getId(),
                        task.getExecutionContext().getExecutionId());
        this.assignmentTracker =
                new SourceAssignmentTracker(
                        config.getAssignmentTrackerMaxEntries(),
                        config.getAssignmentTrackerMaxBytes(),
                        selection.isCheckpointEnabled());
        this.outboundMemoryBudget =
                task.getExecutionContext().getTaskExecutionService().getManagedSourceMemoryBudget();
        this.scheduler =
                new ManagedCoordinatorScheduler(
                        task.getExecutionContext().getTaskExecutionService(),
                        coordinatorEpoch,
                        connectorClassLoader,
                        config.getCoordinatorAsyncMaxConcurrency(),
                        config.getReaderMailboxMaxCommands(),
                        config.getReaderReservedControlCommands(),
                        this::reportAsynchronousFailure,
                        this::admitSchedulerCallback);
    }

    public String getCoordinatorAttemptId() {
        return coordinatorAttemptId;
    }

    public String getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public ManagedCoordinatorScheduler getScheduler() {
        return scheduler;
    }

    /** Returns whether new split discovery is below the assignment tracker's soft watermark. */
    public boolean canAcceptAssignments() {
        checkOwner();
        return !assignmentTracker.isNearCapacity();
    }

    public void setEnumerator(SourceSplitEnumerator<SplitT, Serializable> enumerator) {
        checkOwner();
        this.enumerator = enumerator;
    }

    public SourceCommandAdmissionStatus admitRegistration(ManagedSourceRegistration registration) {
        if (registration == null
                || registration.getReaderLocation() == null
                || registration.getReaderAddress() == null
                || registration.getReaderAttemptId() == null
                || registration.getReaderAttemptId().trim().isEmpty()
                || registration.getReaderAttemptId().length()
                        > SourceCommandEnvelope.MAX_IDENTIFIER_LENGTH) {
            return registrationStatus(SourceCommandAdmissionStatus.INVALID_PAYLOAD);
        }
        if (registration.getRuntimeProtocolVersion() != selection.getRuntimeProtocolVersion()) {
            return registrationStatus(SourceCommandAdmissionStatus.UNSUPPORTED_PROTOCOL);
        }
        if (registration.getReaderLocation().getJobId() != task.getTaskLocation().getJobId()
                || registration.getReaderLocation().getTaskIndex() < 0
                || registration.getReaderLocation().getTaskIndex()
                        >= task.getSourceAction().getParallelism()
                || (selection.getMode().hasManagedReader()
                        && registration.getReaderExecutionId() <= 0)
                || !selection.getCapabilityDigest().equals(registration.getCapabilityDigest())
                || registration.getFirstReaderCommandSequence() <= 0
                || registration.getRestoredAppliedWatermark() < 0
                || registration.getRestoredNoMoreSplitsGeneration() < 0) {
            return registrationStatus(SourceCommandAdmissionStatus.INVALID_PAYLOAD);
        }
        int subtask = registration.getReaderLocation().getTaskIndex();
        synchronized (admissionLock) {
            RegistrationFence current = registrationFences.get(subtask);
            if (current != null) {
                if (registration.getReaderExecutionId() < current.executionId
                        || (registration.getReaderExecutionId() == current.executionId
                                && !registration.getReaderAttemptId().equals(current.attemptId))) {
                    return registrationStatus(SourceCommandAdmissionStatus.STALE_TARGET);
                }
                if (registration.getReaderAttemptId().equals(current.attemptId)) {
                    return registrationStatus(SourceCommandAdmissionStatus.DUPLICATE);
                }
            }
            if (!admitControl(
                    () -> applyRegistration(registration),
                    "reader-registration-" + registration.getReaderAttemptId(),
                    true)) {
                return registrationStatus(SourceCommandAdmissionStatus.RETRY_LATER);
            }
            registrationFences.put(
                    subtask,
                    new RegistrationFence(
                            registration.getReaderExecutionId(),
                            registration.getReaderAttemptId()));
            return registrationStatus(SourceCommandAdmissionStatus.ACCEPTED);
        }
    }

    public void admitLegacyRegistration(TaskLocation readerLocation, Address readerAddress) {
        SourceCommandAdmissionStatus status =
                admitRegistration(
                        new ManagedSourceRegistration(
                                readerLocation,
                                readerAddress,
                                0L,
                                "legacy-reader-" + readerLocation.getTaskID(),
                                selection.getRuntimeProtocolVersion(),
                                selection.getCapabilityDigest(),
                                1L,
                                0L,
                                0L));
        if (status != SourceCommandAdmissionStatus.ACCEPTED
                && status != SourceCommandAdmissionStatus.DUPLICATE) {
            throw new IllegalStateException(
                    "Legacy Source Reader registration rejected: " + status);
        }
    }

    public void admitLegacySplitRequest(int subtask) {
        requireControlAdmission(
                () -> enumerator.handleSplitRequest(subtask),
                "legacy-reader-split-request-" + subtask);
    }

    public void admitLegacySourceEvent(int subtask, SourceEvent event) {
        requireControlAdmission(
                () -> enumerator.handleSourceEvent(subtask, event),
                "legacy-reader-source-event-" + subtask);
    }

    public void admitLegacyReaderFinished(TaskLocation readerLocation) {
        requireControlAdmission(
                () -> task.managedReaderFinished(readerLocation),
                "legacy-reader-finished-" + readerLocation.getTaskID(),
                true);
    }

    public void admitLegacyRestoredSplits(List<SplitT> splits, int subtask) {
        requireControlAdmission(
                () -> {
                    assignmentTracker.markRestoredSplitsIncluded(
                            splits.stream().map(SourceSplit::splitId).collect(Collectors.toSet()));
                    enumerator.addSplitsBack(splits, subtask);
                },
                "legacy-restored-splits-" + subtask);
    }

    /**
     * Defers connector split deserialization and restore callbacks to the coordinator owner thread.
     *
     * @param serializedSplits connector split state bytes received from a failed Reader
     * @param subtask failed Reader subtask
     */
    public void admitLegacySerializedRestoredSplits(List<byte[]> serializedSplits, int subtask) {
        List<byte[]> owned = new ArrayList<>(serializedSplits.size());
        long totalBytes = 0L;
        for (byte[] serializedSplit : serializedSplits) {
            if (serializedSplit == null) {
                throw new IllegalArgumentException(
                        "Managed Source restored split payload must not be null");
            }
            totalBytes = Math.addExact(totalBytes, serializedSplit.length);
            if (totalBytes > config.getReaderMailboxMaxBytes()) {
                throw new IllegalArgumentException(
                        "Managed Source restored split payload exceeds mailbox byte limit");
            }
            owned.add(serializedSplit);
        }
        List<byte[]> immutableOwned = Collections.unmodifiableList(owned);
        if (!outboundMemoryBudget.tryReserve(totalBytes)) {
            throw new IllegalStateException(
                    "Managed Source worker memory budget is exhausted during split restore");
        }
        long reservedBytes = totalBytes;
        boolean admitted =
                admitControl(
                        () -> {
                            try {
                                List<SplitT> splits = new ArrayList<>(immutableOwned.size());
                                for (byte[] serializedSplit : immutableOwned) {
                                    splits.add(splitSerializer.deserialize(serializedSplit));
                                }
                                assignmentTracker.markRestoredSplitsIncluded(
                                        splits.stream()
                                                .map(SourceSplit::splitId)
                                                .collect(Collectors.toSet()));
                                enumerator.addSplitsBack(splits, subtask);
                            } finally {
                                outboundMemoryBudget.release(reservedBytes);
                            }
                        },
                        "legacy-serialized-restored-splits-" + subtask,
                        false,
                        () -> outboundMemoryBudget.release(reservedBytes));
        if (!admitted) {
            outboundMemoryBudget.release(reservedBytes);
            throw new IllegalStateException(
                    "Managed Source coordinator mailbox is full during split restore");
        }
    }

    public SourceCommandAdmissionAck admitReaderCommand(SourceCommandEnvelope command) {
        long startedNanos = System.nanoTime();
        try {
            return admitReaderCommandInternal(command);
        } finally {
            metrics.recordAdmissionDuration(
                    System.nanoTime() - startedNanos,
                    Duration.ofMillis(config.getAdmissionBudgetMillis()).toNanos());
        }
    }

    private SourceCommandAdmissionAck admitReaderCommandInternal(SourceCommandEnvelope command) {
        if (command.getProtocolVersion() != selection.getRuntimeProtocolVersion()) {
            return ack(
                    SourceCommandAdmissionStatus.UNSUPPORTED_PROTOCOL,
                    command,
                    -1L,
                    "Source command protocol mismatch");
        }
        if (command.getJobId() != task.getTaskLocation().getJobId()
                || command.getSourceRuntimeId() != task.getTaskLocation().getTaskID()
                || !coordinatorEpoch.equals(command.getCoordinatorEpoch())
                || !coordinatorAttemptId.equals(command.getTargetAttemptId())) {
            return ack(
                    SourceCommandAdmissionStatus.STALE_TARGET,
                    command,
                    -1L,
                    "Stale Source coordinator epoch or attempt");
        }
        if (!command.hasValidChecksum()
                || command.getPayloadSize() > config.getMaxCommandPayloadBytes()
                || !hasValidReaderCommandHeader(command)) {
            return ack(
                    SourceCommandAdmissionStatus.INVALID_PAYLOAD,
                    command,
                    -1L,
                    "Invalid Source coordinator command payload");
        }
        synchronized (admissionLock) {
            boolean reserved = command.usesReservedCapacity();
            if (!hasControlCapacity(reserved)) {
                return ack(
                        reserved
                                ? SourceCommandAdmissionStatus.TERMINAL_REJECTED
                                : SourceCommandAdmissionStatus.RETRY_LATER,
                        command,
                        -1L,
                        "Source coordinator control mailbox capacity exhausted");
            }
            ReaderCommandMailbox mailbox = inboundMailboxes.get(command.getSenderAttemptId());
            if (mailbox == null) {
                return ack(
                        SourceCommandAdmissionStatus.STALE_TARGET,
                        command,
                        -1L,
                        "Reader attempt is not registered");
            }
            SourceCommandAdmissionAck admission = mailbox.offer(command);
            metrics.recordAdmission(admission.getStatus());
            if (admission.getStatus() == SourceCommandAdmissionStatus.ACCEPTED) {
                int commandBytes = command.estimatedSizeBytes();
                inboundMailboxCommands++;
                inboundMailboxBytes += commandBytes;
                if (reserved) {
                    inboundReservedCommands++;
                    inboundReservedBytes += commandBytes;
                }
                controlEvents.add(
                        new ControlEvent(
                                () -> applyNextReaderCommand(command.getSenderAttemptId()),
                                "reader-command-" + command.getCommandId(),
                                reserved));
                if (!reserved) {
                    normalControlEvents++;
                }
            }
            return admission;
        }
    }

    public void admitBarrier(Barrier barrier) {
        requireControlAdmission(() -> applyBarrier(barrier), "barrier-" + barrier.getId(), true);
    }

    public void admitCheckpointComplete(long checkpointId) {
        requireControlAdmission(
                () -> applyCheckpointComplete(checkpointId),
                "checkpoint-complete-" + checkpointId,
                true);
    }

    public void admitCheckpointAborted(long checkpointId) {
        requireControlAdmission(
                () -> applyCheckpointAborted(checkpointId),
                "checkpoint-aborted-" + checkpointId,
                true);
    }

    public void admitCheckpointEnd(long checkpointId) {
        requireControlAdmission(
                () -> applyCheckpointEnd(checkpointId), "checkpoint-end-" + checkpointId, true);
    }

    /** Executes one connector callback, scheduler result, or admitted Reader command. */
    public boolean runOneTurn() throws Exception {
        checkOwner();
        try {
            return runOneTurnInternal();
        } finally {
            updateMetrics();
        }
    }

    private boolean runOneTurnInternal() throws Exception {
        Throwable failure = asynchronousFailure.getAndSet(null);
        if (failure != null) {
            throw new IllegalStateException(
                    "Managed Source coordinator asynchronous operation failed", failure);
        }
        ControlEvent control;
        synchronized (admissionLock) {
            control = controlEvents.poll();
            if (control != null && !control.reserved) {
                normalControlEvents--;
            }
        }
        if (control != null) {
            control.action.run();
            return true;
        }
        return false;
    }

    public void dispatchSplits(int subtask, List<SplitT> splits) {
        checkOwner();
        if (splits.isEmpty()) {
            return;
        }
        ReaderRegistration reader = requireReadyReader(subtask);
        if (!selection.getMode().hasManagedReader()) {
            dispatchLegacyReaderSplits(reader, splits);
            return;
        }

        List<SerializedSplit> serialized =
                splits.stream().map(this::serializeSplit).collect(Collectors.toList());
        List<List<SerializedSplit>> chunks = chunkAssignments(serialized);
        String assignmentGroupId = UUID.randomUUID().toString();
        for (int chunkIndex = 0; chunkIndex < chunks.size(); chunkIndex++) {
            List<SerializedSplit> chunk = chunks.get(chunkIndex);
            List<String> splitIds =
                    chunk.stream().map(value -> value.splitId).collect(Collectors.toList());
            List<byte[]> splitPayloads =
                    chunk.stream().map(value -> value.payload).collect(Collectors.toList());
            byte[] payload = SourceCommandCodec.encodeSplitAssignment(splitIds, splitPayloads);
            SourceCommandEnvelope command =
                    newReaderCommand(
                            reader,
                            SourceCommandKind.ASSIGN_SPLITS,
                            SourceCommandDurability.CHECKPOINT_COUPLED,
                            SourceCommandCodec.SPLIT_ASSIGNMENT_CODEC,
                            assignmentGroupId,
                            chunkIndex,
                            chunks.size(),
                            payload);
            assignmentTracker.recordDispatched(
                    command.getCommandId(),
                    assignmentGroupId,
                    command.getSenderSequence(),
                    subtask,
                    reader.attemptId,
                    chunkIndex,
                    chunks.size(),
                    splitIds,
                    splitPayloads);
            sendReaderCommand(reader, command);
        }
    }

    public void signalNoMoreSplits(int subtask) {
        checkOwner();
        ReaderRegistration reader = requireReadyReader(subtask);
        noMoreSplitsSubtasks.add(subtask);
        if (noMoreSplitsSubtasks.size() >= task.getSourceAction().getParallelism()) {
            allReadersNoMoreSplits = true;
        }
        if (reader.noMoreSplitsGenerationSent > 0) {
            return;
        }
        sendNoMoreSplits(reader);
    }

    private void sendNoMoreSplits(ReaderRegistration reader) {
        if (!selection.getMode().hasManagedReader()) {
            task.getExecutionContext()
                    .sendToMember(
                            new AssignSplitOperation<>(reader.location, Collections.emptyList()),
                            reader.address);
            reader.noMoreSplitsGenerationSent = 1L;
            return;
        }
        long generation =
                Math.max(
                        Math.addExact(nextNoMoreSplitsGeneration, 1L),
                        Math.addExact(reader.restoredNoMoreSplitsGeneration, 1L));
        nextNoMoreSplitsGeneration = generation;
        SourceCommandEnvelope command =
                newReaderCommand(
                        reader,
                        SourceCommandKind.NO_MORE_SPLITS,
                        SourceCommandDurability.RECONSTRUCTABLE,
                        SourceCommandCodec.NO_MORE_SPLITS_CODEC,
                        "",
                        0,
                        1,
                        SourceCommandCodec.encodeNoMoreSplits(generation));
        reader.noMoreSplitsGenerationSent = generation;
        pendingApplicationAcks.add(command.getCommandId());
        sendReaderCommand(reader, command);
    }

    public void sendEventToReader(int subtask, SourceEvent event) {
        checkOwner();
        if (!selection.getMode().hasManagedReader()) {
            // Preserve the legacy context's historical no-op behavior.
            return;
        }
        throw new UnsupportedOperationException(
                "Managed Source custom events require a versioned event codec, which protocol version 1 does not expose");
    }

    public ManagedCoordinatorCheckpointState restoreRuntimeState(byte[] serialized)
            throws IOException {
        checkOwner();
        ManagedCoordinatorCheckpointState restored =
                ManagedCoordinatorCheckpointStateSerializer.deserialize(serialized);
        validateRestoredSelection(
                restored.getRuntimeMode(),
                restored.getRuntimeProtocolVersion(),
                restored.getConnectorStateVersion(),
                restored.getCapabilityDigest());
        assignmentTracker.restore(
                SourceAssignmentTrackerSerializer.deserialize(
                        restored.getAssignmentTrackerState()));
        int currentParallelism = task.getSourceAction().getParallelism();
        boolean sameParallelism = restored.getSourceParallelism() == currentParallelism;
        nextReaderCommandSequences.clear();
        if (sameParallelism) {
            nextReaderCommandSequences.putAll(restored.getNextReaderCommandSequences());
        }
        noMoreSplitsSubtasks.clear();
        allReadersNoMoreSplits = restored.isAllReadersNoMoreSplits();
        noMoreSplitsSubtasks.addAll(reconcileNoMoreSplitsSubtasks(restored, currentParallelism));
        nextNoMoreSplitsGeneration = restored.getNextNoMoreSplitsGeneration();
        return restored;
    }

    static Set<Integer> reconcileNoMoreSplitsSubtasks(
            ManagedCoordinatorCheckpointState restored, int currentParallelism) {
        if (currentParallelism <= 0) {
            throw new IllegalArgumentException("Managed Source parallelism must be positive");
        }
        Set<Integer> reconciled = new HashSet<>();
        if (restored.isAllReadersNoMoreSplits()) {
            for (int subtask = 0; subtask < currentParallelism; subtask++) {
                reconciled.add(subtask);
            }
        } else if (restored.getSourceParallelism() == currentParallelism) {
            reconciled.addAll(restored.getNoMoreSplitsSubtasks());
        }
        return reconciled;
    }

    @Override
    public void close() {
        closed = true;
        scheduler.close();
        synchronized (admissionLock) {
            inboundMailboxes.values().forEach(ReaderCommandMailbox::close);
            inboundMailboxes.clear();
            controlEvents.forEach(ControlEvent::discard);
            controlEvents.clear();
            normalControlEvents = 0;
            inboundMailboxCommands = 0;
            inboundMailboxBytes = 0L;
            inboundReservedCommands = 0;
            inboundReservedBytes = 0L;
            registrationFences.clear();
        }
        schedulerDrainAdmitted.set(false);
        readers.values().forEach(this::releaseAllOutbound);
        readers.clear();
        registrationDeadlineTimers.clear();
        checkpointReports.values().forEach(CheckpointReportAccumulator::close);
        checkpointReports.clear();
        restoredSplitReports.values().forEach(SplitIdChunkAccumulator::close);
        restoredSplitReports.clear();
        pendingApplicationAcks.clear();
        deferredSplitRequests.clear();
        transportRetries.clear();
        transportFirstAttemptNanos.clear();
    }

    private void applyRegistration(ManagedSourceRegistration registration) throws Exception {
        checkOwner();
        if (closed) {
            return;
        }
        int subtask = registration.getReaderLocation().getTaskIndex();
        synchronized (admissionLock) {
            RegistrationFence fence = registrationFences.get(subtask);
            if (fence == null
                    || fence.executionId != registration.getReaderExecutionId()
                    || !fence.attemptId.equals(registration.getReaderAttemptId())) {
                return;
            }
        }
        ReaderRegistration existing = readers.get(subtask);
        if (existing != null) {
            if (registration.getReaderExecutionId() < existing.executionId
                    || (registration.getReaderExecutionId() == existing.executionId
                            && !existing.attemptId.equals(registration.getReaderAttemptId()))) {
                return;
            }
            if (existing.attemptId.equals(registration.getReaderAttemptId())) {
                return;
            }
            releaseAllOutbound(existing);
            clearCheckpointReportsForReader(existing.attemptId);
            SplitIdChunkAccumulator oldRestore = restoredSplitReports.remove(existing.attemptId);
            if (oldRestore != null) {
                oldRestore.close();
            }
            synchronized (admissionLock) {
                ReaderCommandMailbox old = inboundMailboxes.remove(existing.attemptId);
                if (old != null) {
                    removeInboundMailboxAccounting(old);
                    old.close();
                }
            }
        }
        Cancellable deadline = registrationDeadlineTimers.remove(subtask);
        if (deadline != null) {
            deadline.cancel();
        }
        ReaderRegistration reader =
                new ReaderRegistration(
                        registration.getReaderLocation(),
                        registration.getReaderAddress(),
                        registration.getReaderExecutionId(),
                        registration.getReaderAttemptId(),
                        registration.getRestoredAppliedWatermark(),
                        registration.getRestoredNoMoreSplitsGeneration());
        readers.put(subtask, reader);
        nextReaderCommandSequences.put(subtask, 1L);
        synchronized (admissionLock) {
            inboundMailboxes.put(
                    reader.attemptId,
                    new ReaderCommandMailbox(
                            config,
                            task.getExecutionContext()
                                    .getTaskExecutionService()
                                    .getManagedSourceMemoryBudget(),
                            registration.getFirstReaderCommandSequence()));
        }
        task.addTaskMemberMapping(reader.location, reader.address);
        if (selection.getMode().hasManagedReader()) {
            sendEpochStart(reader);
        } else {
            completeReaderRegistration(reader);
        }
    }

    private void sendEpochStart(ReaderRegistration reader) {
        SourceCommandEnvelope epochStart =
                newReaderCommand(
                        reader,
                        SourceCommandKind.READER_EPOCH_START,
                        SourceCommandDurability.CHECKPOINT_COUPLED,
                        SourceCommandCodec.EMPTY_CODEC,
                        "",
                        0,
                        1,
                        new byte[0]);
        reader.epochCommandId = epochStart.getCommandId();
        reader.ready = false;
        sendReaderCommand(reader, epochStart);
    }

    private void applyReaderCommand(SourceCommandEnvelope command) throws Exception {
        ReaderRegistration reader = findReader(command.getSenderAttemptId());
        switch (command.getKind()) {
            case REQUEST_SPLIT:
                if (assignmentTracker.isNearCapacity()) {
                    if (deferredSplitRequests.add(reader.location.getTaskIndex())
                            && assignmentBackpressureStartNanos == 0L) {
                        assignmentBackpressureStartNanos = System.nanoTime();
                    }
                } else {
                    enumerator.handleSplitRequest(reader.location.getTaskIndex());
                }
                break;
            case READER_SOURCE_EVENT:
                throw new UnsupportedOperationException(
                        "Managed Source protocol version 1 does not support custom Reader events");
            case READER_FINISHED:
                task.managedReaderFinished(reader.location);
                break;
            case COMMAND_APPLIED:
                SourceCommandCodec.CommandApplied applied =
                        SourceCommandCodec.decodeCommandApplied(command.getPayload());
                if (applied.getCommandId().equals(reader.epochCommandId)) {
                    if (restoredSplitReports.containsKey(reader.attemptId)) {
                        throw new IllegalStateException(
                                "Managed Source Reader applied its epoch before all restore chunks");
                    }
                    reader.epochApplied = true;
                    completeManagedRegistrationsIfReady();
                } else if (assignmentTracker.contains(applied.getCommandId())) {
                    assignmentTracker.markApplied(
                            applied.getCommandId(), reader.attemptId, applied.getSplitIds());
                    drainDeferredSplitRequests();
                } else if (!pendingApplicationAcks.remove(applied.getCommandId())) {
                    throw new IllegalStateException(
                            "Unknown managed Source command application " + applied.getCommandId());
                }
                break;
            case READER_CHECKPOINT_REPORT:
                applyReaderCheckpointReport(reader, command);
                break;
            case RESTORED_SPLITS:
                applyRestoredSplits(reader, command);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Reader-to-coordinator Source command " + command.getKind());
        }
    }

    private void applyReaderCheckpointReport(
            ReaderRegistration reader, SourceCommandEnvelope command) throws IOException {
        SourceCommandCodec.ReaderCheckpointReport report =
                SourceCommandCodec.decodeReaderCheckpointReport(command.getPayload());
        if (completedCheckpoints.contains(report.getCheckpointId())
                || abortedCheckpoints.contains(report.getCheckpointId())) {
            return;
        }
        String key =
                reader.attemptId
                        + ":"
                        + report.getCheckpointId()
                        + ":"
                        + command.getAssignmentGroupId();
        CheckpointReportAccumulator accumulator =
                checkpointReports.computeIfAbsent(
                        key,
                        ignored -> {
                            long reportLimit =
                                    Math.min(
                                            config.getAssignmentTrackerMaxEntries(),
                                            (long) task.getSourceAction().getParallelism()
                                                    * MAX_CHECKPOINT_HISTORY);
                            if (checkpointReports.size() >= reportLimit) {
                                throw new IllegalStateException(
                                        "Managed Source checkpoint report accumulator capacity exhausted");
                            }
                            return new CheckpointReportAccumulator(
                                    reader.attemptId,
                                    command.getAssignmentGroupId(),
                                    command.getChunkCount(),
                                    report.getCheckpointId(),
                                    report.getAppliedWatermark(),
                                    config.getAssignmentTrackerMaxBytes(),
                                    outboundMemoryBudget);
                        });
        accumulator.add(
                command.getAssignmentGroupId(),
                command.getChunkCount(),
                report.getCheckpointId(),
                report.getAppliedWatermark(),
                command.getChunkIndex(),
                report.getSplitIds());
        if (accumulator.complete()) {
            try {
                assignmentTracker.markReaderCheckpointIncluded(
                        reader.location.getTaskIndex(),
                        reader.attemptId,
                        accumulator.checkpointId,
                        accumulator.appliedWatermark,
                        accumulator.splitIds());
            } finally {
                checkpointReports.remove(key);
                accumulator.close();
            }
        }
    }

    private void applyRestoredSplits(ReaderRegistration reader, SourceCommandEnvelope command)
            throws Exception {
        SourceCommandCodec.RestoredSplits restored =
                SourceCommandCodec.decodeRestoredSplits(command.getPayload());
        List<SplitT> splits = new ArrayList<>(restored.getConnectorSplitStates().size());
        for (byte[] connectorSplitState : restored.getConnectorSplitStates()) {
            splits.add(splitSerializer.deserialize(connectorSplitState));
        }
        if (!splits.isEmpty()) {
            enumerator.addSplitsBack(splits, reader.location.getTaskIndex());
        }
        SplitIdChunkAccumulator accumulator =
                restoredSplitReports.computeIfAbsent(
                        reader.attemptId,
                        ignored ->
                                new SplitIdChunkAccumulator(
                                        command.getAssignmentGroupId(),
                                        command.getChunkCount(),
                                        config.getAssignmentTrackerMaxBytes(),
                                        outboundMemoryBudget));
        accumulator.add(
                command.getAssignmentGroupId(),
                command.getChunkCount(),
                command.getChunkIndex(),
                restored.getCheckpointOwnedSplitIds());
        if (accumulator.complete()) {
            try {
                assignmentTracker.markRestoredSplitsIncluded(accumulator.splitIds());
            } finally {
                restoredSplitReports.remove(reader.attemptId);
                accumulator.close();
            }
        }
    }

    private void applyBarrier(Barrier barrier) throws Exception {
        checkOwner();
        if (barrier.prepareClose(task.getTaskLocation())) {
            task.managedPrepareCloseTriggered(barrier.getId());
        }
        Serializable connectorSnapshot = null;
        byte[] connectorState = new byte[0];
        if (barrier.snapshot()) {
            connectorSnapshot = enumerator.snapshotState(barrier.getId());
            connectorState = enumeratorStateSerializer.serialize(connectorSnapshot);
        }

        for (ReaderRegistration reader : readers.values()) {
            if (barrier.closedTasks().contains(reader.location)) {
                continue;
            }
            if (selection.getMode().hasManagedReader()) {
                sendReaderCommand(
                        reader,
                        newReaderCommand(
                                reader,
                                SourceCommandKind.BARRIER,
                                SourceCommandDurability.CHECKPOINT_COUPLED,
                                SourceCommandCodec.BARRIER_CODEC,
                                "",
                                0,
                                1,
                                SourceCommandCodec.encodeBarrier(barrier)));
            } else {
                task.getExecutionContext()
                        .sendToMember(
                                new BarrierFlowOperation(barrier, reader.location), reader.address);
            }
        }

        if (barrier.snapshot()) {
            byte[] trackerState =
                    SourceAssignmentTrackerSerializer.serialize(assignmentTracker.entries());
            ManagedCoordinatorCheckpointState runtimeState =
                    new ManagedCoordinatorCheckpointState(
                            selection.getMode(),
                            selection.getRuntimeProtocolVersion(),
                            selection.getConnectorStateVersion(),
                            selection.getCapabilityDigest(),
                            task.getSourceAction().getParallelism(),
                            connectorState,
                            trackerState,
                            nextReaderCommandSequences,
                            noMoreSplitsSubtasks,
                            allReadersNoMoreSplits,
                            nextNoMoreSplitsGeneration);
            byte[] serialized = ManagedCoordinatorCheckpointStateSerializer.serialize(runtimeState);
            task.getExecutionContext()
                    .sendToMaster(
                            new TaskAcknowledgeOperation(
                                    task.getTaskLocation(),
                                    (CheckpointBarrier) barrier,
                                    Collections.singletonList(
                                            new ActionSubtaskState(
                                                    ActionStateKey.of(task.getSourceAction()),
                                                    -1,
                                                    Collections.singletonList(serialized)))))
                    .whenComplete(
                            (ignored, failure) -> {
                                if (failure != null) {
                                    reportAsynchronousFailure(failure);
                                }
                            });
        }
    }

    private void applyCheckpointComplete(long checkpointId) throws Exception {
        if (abortedCheckpoints.contains(checkpointId) || !completedCheckpoints.add(checkpointId)) {
            return;
        }
        trimCheckpointHistory();
        clearCheckpointReportsThrough(checkpointId);
        enumerator.notifyCheckpointComplete(checkpointId);
        assignmentTracker.checkpointCompleted(checkpointId);
        drainDeferredSplitRequests();
        sendCheckpointCallback(SourceCommandKind.CHECKPOINT_COMPLETE, checkpointId);
        task.managedCheckpointCallbackFinished(checkpointId);
    }

    private void applyCheckpointAborted(long checkpointId) throws Exception {
        if (completedCheckpoints.contains(checkpointId) || !abortedCheckpoints.add(checkpointId)) {
            return;
        }
        trimCheckpointHistory();
        clearCheckpointReportsThrough(checkpointId);
        enumerator.notifyCheckpointAborted(checkpointId);
        sendCheckpointCallback(SourceCommandKind.CHECKPOINT_ABORTED, checkpointId);
        task.managedCheckpointCallbackFinished(checkpointId);
    }

    private void applyCheckpointEnd(long checkpointId) {
        sendCheckpointCallback(SourceCommandKind.CHECKPOINT_END, checkpointId);
    }

    private void sendCheckpointCallback(SourceCommandKind kind, long checkpointId) {
        if (!selection.getMode().hasManagedReader()) {
            return;
        }
        byte[] payload = SourceCommandCodec.encodeCheckpointId(checkpointId);
        for (ReaderRegistration reader : readers.values()) {
            if (reader.ready) {
                sendReaderCommand(
                        reader,
                        newReaderCommand(
                                reader,
                                kind,
                                SourceCommandDurability.CHECKPOINT_COUPLED,
                                SourceCommandCodec.CHECKPOINT_ID_CODEC,
                                "",
                                0,
                                1,
                                payload));
            }
        }
    }

    private void applyNextReaderCommand(String readerAttemptId) throws Exception {
        SourceCommandEnvelope command;
        synchronized (admissionLock) {
            ReaderCommandMailbox mailbox = inboundMailboxes.get(readerAttemptId);
            if (mailbox == null) {
                return;
            }
            command = mailbox.pollNext();
            if (command != null) {
                int commandBytes = command.estimatedSizeBytes();
                inboundMailboxCommands--;
                inboundMailboxBytes -= commandBytes;
                if (command.usesReservedCapacity()) {
                    inboundReservedCommands--;
                    inboundReservedBytes -= commandBytes;
                }
                checkInboundAccounting();
            }
        }
        if (command == null) {
            throw new IllegalStateException(
                    "Managed Source coordinator command sequence has a gap for reader attempt "
                            + readerAttemptId);
        }
        long startedNanos = System.nanoTime();
        try {
            applyReaderCommand(command);
        } finally {
            metrics.recordCommand(
                    command.getAdmittedNanos() <= 0
                            ? 0L
                            : Math.max(0L, startedNanos - command.getAdmittedNanos()),
                    Math.max(0L, System.nanoTime() - startedNanos));
        }
    }

    private SourceCommandEnvelope newReaderCommand(
            ReaderRegistration reader,
            SourceCommandKind kind,
            SourceCommandDurability durability,
            int codecId,
            String groupId,
            int chunkIndex,
            int chunkCount,
            byte[] payload) {
        long sequence =
                nextReaderCommandSequences.compute(
                        reader.location.getTaskIndex(),
                        (ignored, current) -> current == null ? 2L : current + 1L);
        // The map stores the next sequence. The first command therefore uses next - 1.
        long commandSequence = sequence - 1L;
        return SourceCommandEnvelope.create(
                task.getTaskLocation().getJobId(),
                task.getTaskLocation().getTaskID(),
                coordinatorEpoch,
                coordinatorAttemptId,
                reader.attemptId,
                commandSequence,
                kind,
                durability,
                SourceCommandCodec.PAYLOAD_VERSION,
                codecId,
                groupId,
                chunkIndex,
                chunkCount,
                payload);
    }

    private void sendReaderCommand(ReaderRegistration reader, SourceCommandEnvelope command) {
        reserveOutbound(reader, command);
        reader.pendingOutbound.add(command);
        sendNextReaderCommand(reader);
    }

    private void reserveOutbound(ReaderRegistration reader, SourceCommandEnvelope command) {
        int bytes = command.estimatedSizeBytes();
        boolean reserved = command.usesReservedCapacity();
        int normalMaxCommands =
                config.getReaderMailboxMaxCommands() - config.getReaderReservedControlCommands();
        long normalMaxBytes =
                config.getReaderMailboxMaxBytes() - config.getReaderReservedControlBytes();
        if (reader.outboundCommands >= config.getReaderMailboxMaxCommands()
                || bytes > config.getReaderMailboxMaxBytes() - reader.outboundBytes
                || (!reserved
                        && (reader.normalOutboundCommands >= normalMaxCommands
                                || bytes > normalMaxBytes - reader.normalOutboundBytes))) {
            throw new IllegalStateException(
                    "Managed Source outbound command window is full for reader "
                            + reader.location.getTaskIndex());
        }
        if (!outboundMemoryBudget.tryReserve(bytes)) {
            throw new IllegalStateException(
                    "Managed Source worker outbound memory budget is exhausted");
        }
        reader.outboundCommands++;
        reader.outboundBytes += bytes;
        outboundCommands++;
        outboundBytes += bytes;
        if (!reserved) {
            reader.normalOutboundCommands++;
            reader.normalOutboundBytes += bytes;
        }
    }

    private void sendNextReaderCommand(ReaderRegistration reader) {
        if (reader.inFlightCommand != null) {
            return;
        }
        reader.inFlightCommand = reader.pendingOutbound.poll();
        if (reader.inFlightCommand != null) {
            invokeReaderCommand(reader, reader.inFlightCommand);
        }
    }

    private void invokeReaderCommand(ReaderRegistration reader, SourceCommandEnvelope command) {
        transportFirstAttemptNanos.putIfAbsent(command.getCommandId(), System.nanoTime());
        InvocationFuture<?> future =
                task.getExecutionContext()
                        .sendToMember(
                                new ManagedSourceCommandOperation(reader.location, command),
                                reader.address);
        future.whenComplete(
                (result, failure) ->
                        requireControlAdmission(
                                () ->
                                        handleTransportResult(
                                                reader.attemptId,
                                                command,
                                                result instanceof SourceCommandAdmissionAck
                                                        ? (SourceCommandAdmissionAck) result
                                                        : null,
                                                failure),
                                "transport-result-" + command.getCommandId(),
                                true));
    }

    private void completeOutboundCommand(ReaderRegistration reader, SourceCommandEnvelope command) {
        if (reader.inFlightCommand == null
                || !reader.inFlightCommand.getCommandId().equals(command.getCommandId())) {
            return;
        }
        releaseOutbound(reader, command);
        reader.inFlightCommand = null;
        sendNextReaderCommand(reader);
    }

    private void releaseOutbound(ReaderRegistration reader, SourceCommandEnvelope command) {
        int bytes = command.estimatedSizeBytes();
        reader.outboundCommands--;
        reader.outboundBytes -= bytes;
        outboundCommands--;
        outboundBytes -= bytes;
        if (!command.usesReservedCapacity()) {
            reader.normalOutboundCommands--;
            reader.normalOutboundBytes -= bytes;
        }
        if (reader.outboundCommands < 0
                || reader.outboundBytes < 0
                || reader.normalOutboundCommands < 0
                || reader.normalOutboundBytes < 0
                || outboundCommands < 0
                || outboundBytes < 0) {
            throw new IllegalStateException("Managed Source outbound command accounting underflow");
        }
        outboundMemoryBudget.release(bytes);
    }

    private void releaseAllOutbound(ReaderRegistration reader) {
        reader.pendingOutbound.stream()
                .filter(command -> command.getKind() == SourceCommandKind.NO_MORE_SPLITS)
                .map(SourceCommandEnvelope::getCommandId)
                .forEach(pendingApplicationAcks::remove);
        if (reader.inFlightCommand != null
                && reader.inFlightCommand.getKind() == SourceCommandKind.NO_MORE_SPLITS) {
            pendingApplicationAcks.remove(reader.inFlightCommand.getCommandId());
        }
        if (reader.outboundBytes > 0) {
            outboundMemoryBudget.release(reader.outboundBytes);
        }
        outboundCommands -= reader.outboundCommands;
        outboundBytes -= reader.outboundBytes;
        if (outboundCommands < 0 || outboundBytes < 0) {
            throw new IllegalStateException(
                    "Managed Source aggregate outbound accounting underflow");
        }
        reader.pendingOutbound.clear();
        reader.inFlightCommand = null;
        reader.outboundCommands = 0;
        reader.outboundBytes = 0L;
        reader.normalOutboundCommands = 0;
        reader.normalOutboundBytes = 0L;
    }

    private void handleTransportResult(
            String readerAttemptId,
            SourceCommandEnvelope command,
            SourceCommandAdmissionAck ack,
            Throwable failure) {
        checkOwner();
        ReaderRegistration reader = readers.get(commandTargetSubtask(readerAttemptId));
        if (reader == null || !reader.attemptId.equals(readerAttemptId)) {
            return;
        }
        if (reader.inFlightCommand == null
                || !reader.inFlightCommand.getCommandId().equals(command.getCommandId())) {
            return;
        }
        if (failure != null || ack == null) {
            scheduleTransportRetry(reader, command, config.getRetryInitialBackoffMillis());
            return;
        }
        switch (ack.getStatus()) {
            case ACCEPTED:
            case DUPLICATE:
                transportRetries.remove(command.getCommandId());
                transportFirstAttemptNanos.remove(command.getCommandId());
                if (command.getKind() == SourceCommandKind.ASSIGN_SPLITS) {
                    assignmentTracker.markAdmitted(command.getCommandId(), reader.attemptId);
                }
                completeOutboundCommand(reader, command);
                return;
            case RETRY_LATER:
                scheduleTransportRetry(
                        reader,
                        command,
                        Math.max(config.getRetryInitialBackoffMillis(), ack.getRetryAfterMillis()));
                return;
            case STALE_TARGET:
                transportRetries.remove(command.getCommandId());
                transportFirstAttemptNanos.remove(command.getCommandId());
                reader.ready = false;
                metrics.recordAdmission(SourceCommandAdmissionStatus.STALE_TARGET);
                scheduleRegistrationDeadline(reader);
                return;
            case TERMINAL_REJECTED:
            case UNSUPPORTED_PROTOCOL:
            case INVALID_PAYLOAD:
                throw new IllegalStateException(
                        "Managed Source command "
                                + command.getCommandId()
                                + " rejected: "
                                + ack.getStatus()
                                + " "
                                + ack.getDetail());
            default:
                throw new IllegalArgumentException(
                        "Unknown managed Source admission status " + ack.getStatus());
        }
    }

    private void scheduleTransportRetry(
            ReaderRegistration reader, SourceCommandEnvelope command, long suggestedBackoffMillis) {
        long nowNanos = System.nanoTime();
        long firstAttemptNanos =
                transportFirstAttemptNanos.computeIfAbsent(
                        command.getCommandId(), ignored -> nowNanos);
        long elapsedMillis =
                Duration.ofNanos(Math.max(0L, nowNanos - firstAttemptNanos)).toMillis();
        if (elapsedMillis >= config.getCommandRetryDeadlineMillis()) {
            transportRetries.remove(command.getCommandId());
            transportFirstAttemptNanos.remove(command.getCommandId());
            if (command.getDurability() == SourceCommandDurability.EPHEMERAL) {
                completeOutboundCommand(reader, command);
                return;
            }
            throw new IllegalStateException(
                    "Managed Source command "
                            + command.getCommandId()
                            + " exceeded retry deadline after "
                            + elapsedMillis
                            + " ms");
        }
        int retry = transportRetries.merge(command.getCommandId(), 1, Integer::sum);
        metrics.recordTransportRetry();
        long exponential =
                config.getRetryInitialBackoffMillis()
                        * (1L << Math.min(20, Math.max(0, retry - 1)));
        long computedBackoff =
                Math.min(
                        config.getRetryMaxBackoffMillis(),
                        Math.max(suggestedBackoffMillis, exponential));
        long backoff =
                jitterBackoff(Math.min(computedBackoff, suggestedBackoffMillis), computedBackoff);
        scheduler.scheduleInCoordinatorThread(
                AsyncTaskKey.of("transport-retry-" + command.getCommandId()),
                Duration.ofMillis(backoff),
                () -> {
                    ReaderRegistration current = readers.get(reader.location.getTaskIndex());
                    if (current != null
                            && current.attemptId.equals(reader.attemptId)
                            && current.inFlightCommand != null
                            && current.inFlightCommand.getCommandId().equals(command.getCommandId())
                            && !closed) {
                        invokeReaderCommand(current, command);
                    }
                });
    }

    private void completeReaderRegistration(ReaderRegistration reader) {
        if (reader.ready) {
            return;
        }
        reader.ready = true;
        enumerator.registerReader(reader.location.getTaskIndex());
        replayNoMoreSplits(reader);
        task.managedReaderRegistered(
                (int) readers.values().stream().filter(value -> value.ready).count());
    }

    /**
     * Completes managed registration only after every current Reader has applied its epoch.
     *
     * <p>Each Reader sends restore chunks before its epoch application proof. Waiting for all
     * proofs prevents a rescaled Reader from replaying an assignment whose checkpoint state is
     * still in transit from another old subtask.
     */
    private void completeManagedRegistrationsIfReady() throws Exception {
        if (readers.size() != task.getSourceAction().getParallelism()
                || readers.values().stream()
                        .anyMatch(reader -> !reader.ready && !reader.epochApplied)) {
            return;
        }
        long startedNanos = System.nanoTime();
        reconcileAssignmentsForMissingReaders();
        List<ReaderRegistration> pending =
                readers.values().stream()
                        .filter(reader -> !reader.ready)
                        .collect(Collectors.toList());
        pending.forEach(reader -> reader.ready = true);
        for (ReaderRegistration reader : pending) {
            enumerator.registerReader(reader.location.getTaskIndex());
            replayAssignments(reader);
            replayNoMoreSplits(reader);
        }
        task.managedReaderRegistered(
                (int) readers.values().stream().filter(value -> value.ready).count());
        metrics.recordRegistrationReconciliation(Math.max(0L, System.nanoTime() - startedNanos));
    }

    /** Returns uncheckpointed assignments for removed subtasks to the connector enumerator. */
    private void reconcileAssignmentsForMissingReaders() throws Exception {
        Set<Integer> activeSubtasks = new HashSet<>(readers.keySet());
        for (SourceAssignmentTracker.Entry entry :
                assignmentTracker.takeAssignmentsForMissingReaders(activeSubtasks)) {
            List<byte[]> splitPayloads = entry.getSplitPayloads();
            List<SplitT> splits = new ArrayList<>(splitPayloads.size());
            for (int i = 0; i < splitPayloads.size(); i++) {
                SplitT split = splitSerializer.deserialize(splitPayloads.get(i));
                if (!entry.getSplitIds().get(i).equals(split.splitId())) {
                    throw new IOException(
                            "Orphaned managed Source assignment split identifier mismatch");
                }
                splits.add(split);
            }
            int reassignmentSubtask =
                    Math.floorMod(
                            entry.getTargetSubtask(), task.getSourceAction().getParallelism());
            enumerator.addSplitsBack(splits, reassignmentSubtask);
        }
    }

    private void replayAssignments(ReaderRegistration reader) {
        for (SourceAssignmentTracker.Entry entry :
                assignmentTracker.assignmentsForReader(reader.location.getTaskIndex())) {
            byte[] payload =
                    SourceCommandCodec.encodeSplitAssignment(
                            entry.getSplitIds(), entry.getSplitPayloads());
            long sequence =
                    nextReaderCommandSequences.compute(
                                    reader.location.getTaskIndex(),
                                    (ignored, current) -> current == null ? 2L : current + 1L)
                            - 1L;
            assignmentTracker.rebindForReplay(entry.getCommandId(), reader.attemptId, sequence);
            SourceCommandEnvelope replay =
                    new SourceCommandEnvelope(
                            selection.getRuntimeProtocolVersion(),
                            task.getTaskLocation().getJobId(),
                            task.getTaskLocation().getTaskID(),
                            coordinatorEpoch,
                            coordinatorAttemptId,
                            reader.attemptId,
                            sequence,
                            entry.getCommandId(),
                            SourceCommandKind.ASSIGN_SPLITS,
                            SourceCommandDurability.CHECKPOINT_COUPLED,
                            SourceCommandCodec.PAYLOAD_VERSION,
                            SourceCommandCodec.SPLIT_ASSIGNMENT_CODEC,
                            entry.getAssignmentGroupId(),
                            entry.getChunkIndex(),
                            entry.getChunkCount(),
                            checksum(payload),
                            payload,
                            0L);
            sendReaderCommand(reader, replay);
        }
    }

    private void replayNoMoreSplits(ReaderRegistration reader) {
        int subtask = reader.location.getTaskIndex();
        if (allReadersNoMoreSplits || noMoreSplitsSubtasks.contains(subtask)) {
            noMoreSplitsSubtasks.add(subtask);
            sendNoMoreSplits(reader);
        }
    }

    private void drainDeferredSplitRequests() {
        java.util.Iterator<Integer> iterator = deferredSplitRequests.iterator();
        while (!assignmentTracker.isNearCapacity() && iterator.hasNext()) {
            int subtask = iterator.next();
            iterator.remove();
            ReaderRegistration reader = readers.get(subtask);
            if (reader != null && reader.ready) {
                enumerator.handleSplitRequest(subtask);
            }
        }
        if (deferredSplitRequests.isEmpty() && assignmentBackpressureStartNanos != 0L) {
            metrics.recordAssignmentBackpressure(
                    Math.max(0L, System.nanoTime() - assignmentBackpressureStartNanos));
            assignmentBackpressureStartNanos = 0L;
        }
    }

    /** Bounds duplicate and stale checkpoint callback tracking. */
    private void trimCheckpointHistory() {
        while (completedCheckpoints.size() > MAX_CHECKPOINT_HISTORY) {
            completedCheckpoints.remove(Collections.min(completedCheckpoints));
        }
        while (abortedCheckpoints.size() > MAX_CHECKPOINT_HISTORY) {
            abortedCheckpoints.remove(Collections.min(abortedCheckpoints));
        }
    }

    private void dispatchLegacyReaderSplits(ReaderRegistration reader, List<SplitT> splits) {
        List<byte[]> payloads =
                splits.stream()
                        .map(split -> serializeSplit(split).payload)
                        .collect(Collectors.toList());
        task.getExecutionContext()
                .sendToMember(
                        new AssignSplitOperation<>(reader.location, payloads), reader.address);
    }

    private List<List<SerializedSplit>> chunkAssignments(List<SerializedSplit> splits) {
        List<List<SerializedSplit>> chunks = new ArrayList<>();
        List<SerializedSplit> current = new ArrayList<>();
        int currentBytes = SourceCommandCodec.splitAssignmentBaseSize();
        for (SerializedSplit split : splits) {
            int entryBytes =
                    SourceCommandCodec.splitAssignmentEntrySize(split.splitId, split.payload);
            if (entryBytes
                    > config.getMaxCommandPayloadBytes()
                            - SourceCommandCodec.splitAssignmentBaseSize()) {
                throw new IllegalArgumentException(
                        "Serialized Source split "
                                + split.splitId
                                + " exceeds max-command-payload-bytes");
            }
            if (!current.isEmpty()
                    && (current.size() >= SourceCommandCodec.maxCollectionSize()
                            || entryBytes > config.getMaxCommandPayloadBytes() - currentBytes)) {
                chunks.add(current);
                current = new ArrayList<>();
                currentBytes = SourceCommandCodec.splitAssignmentBaseSize();
            }
            current.add(split);
            currentBytes = Math.addExact(currentBytes, entryBytes);
        }
        if (!current.isEmpty()) {
            chunks.add(current);
        }
        return chunks;
    }

    private SerializedSplit serializeSplit(SplitT split) {
        try {
            return new SerializedSplit(split.splitId(), splitSerializer.serialize(split));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to serialize Source split " + split.splitId(), e);
        }
    }

    private List<SplitT> deserializeSplits(SourceCommandCodec.SplitAssignment assignment)
            throws Exception {
        List<SplitT> splits = new ArrayList<>(assignment.getSplitBytes().size());
        for (int i = 0; i < assignment.getSplitBytes().size(); i++) {
            SplitT split = splitSerializer.deserialize(assignment.getSplitBytes().get(i));
            if (!assignment.getSplitIds().get(i).equals(split.splitId())) {
                throw new IOException("Restored Source split identifier mismatch");
            }
            splits.add(split);
        }
        return splits;
    }

    private ReaderRegistration requireReadyReader(int subtask) {
        ReaderRegistration reader = readers.get(subtask);
        if (reader == null || !reader.ready) {
            throw new IllegalStateException(
                    "Source reader " + subtask + " has no ready managed attempt");
        }
        return reader;
    }

    private ReaderRegistration findReader(String attemptId) {
        for (ReaderRegistration reader : readers.values()) {
            if (reader.attemptId.equals(attemptId)) {
                return reader;
            }
        }
        throw new IllegalStateException("Stale managed Source Reader attempt " + attemptId);
    }

    private int commandTargetSubtask(String readerAttemptId) {
        for (Map.Entry<Integer, ReaderRegistration> entry : readers.entrySet()) {
            if (entry.getValue().attemptId.equals(readerAttemptId)) {
                return entry.getKey();
            }
        }
        return -1;
    }

    private boolean admitControl(CheckedRunnable action, String description, boolean reserved) {
        return admitControl(action, description, reserved, () -> {});
    }

    private boolean admitControl(
            CheckedRunnable action, String description, boolean reserved, Runnable discardAction) {
        synchronized (admissionLock) {
            if (closed || !hasControlCapacity(reserved)) {
                return false;
            }
            controlEvents.add(new ControlEvent(action, description, reserved, discardAction));
            if (!reserved) {
                normalControlEvents++;
            }
            return true;
        }
    }

    private boolean admitControl(CheckedRunnable action, String description) {
        return admitControl(action, description, false);
    }

    private boolean hasControlCapacity(boolean reserved) {
        int maxCommands = config.getReaderMailboxMaxCommands();
        int normalMaxCommands = maxCommands - config.getReaderReservedControlCommands();
        return controlEvents.size() < maxCommands
                && (reserved || normalControlEvents < normalMaxCommands);
    }

    private void admitSchedulerCallback(boolean ignoredReserved) {
        if (!schedulerDrainAdmitted.compareAndSet(false, true)) {
            return;
        }
        if (!admitControl(
                this::drainSchedulerCallback, "managed-coordinator-scheduler-callback", true)) {
            schedulerDrainAdmitted.set(false);
            reportAsynchronousFailure(
                    new IllegalStateException(
                            "Managed Source coordinator reserved control mailbox exhausted: "
                                    + "managed-coordinator-scheduler-callback"));
        }
    }

    private void drainSchedulerCallback() {
        schedulerDrainAdmitted.set(false);
        scheduler.drainOneCallback();
        if (scheduler.hasPendingCallbacks()) {
            admitSchedulerCallback(true);
        }
    }

    private void requireControlAdmission(CheckedRunnable action, String description) {
        if (!admitControl(action, description)) {
            reportAsynchronousFailure(
                    new IllegalStateException(
                            "Managed Source coordinator control mailbox exhausted: "
                                    + description));
        }
    }

    private void requireControlAdmission(
            CheckedRunnable action, String description, boolean reserved) {
        if (!admitControl(action, description, reserved)) {
            reportAsynchronousFailure(
                    new IllegalStateException(
                            "Managed Source coordinator reserved control mailbox exhausted: "
                                    + description));
        }
    }

    private void reportAsynchronousFailure(Throwable throwable) {
        asynchronousFailure.compareAndSet(null, throwable);
    }

    /**
     * Validates Reader-to-coordinator payload identity without deserializing connector objects on
     * the Hazelcast operation thread.
     */
    private boolean hasValidReaderCommandHeader(SourceCommandEnvelope command) {
        if (command.getPayloadVersion() != SourceCommandCodec.PAYLOAD_VERSION) {
            return false;
        }
        switch (command.getKind()) {
            case REQUEST_SPLIT:
            case READER_FINISHED:
                return command.getCodecId() == SourceCommandCodec.EMPTY_CODEC
                        && command.getPayloadSize() == 0;
            case COMMAND_APPLIED:
                return command.getCodecId() == SourceCommandCodec.COMMAND_APPLIED_CODEC;
            case READER_CHECKPOINT_REPORT:
                return command.getCodecId() == SourceCommandCodec.READER_CHECKPOINT_REPORT_CODEC
                        && !command.getAssignmentGroupId().isEmpty();
            case RESTORED_SPLITS:
                return command.getCodecId() == SourceCommandCodec.RESTORED_SPLITS_CODEC
                        && !command.getAssignmentGroupId().isEmpty();
            case READER_SOURCE_EVENT:
                return false;
            default:
                return false;
        }
    }

    private void validateRestoredSelection(
            org.apache.seatunnel.engine.common.runtime.source.ManagedSourceRuntimeMode mode,
            int protocolVersion,
            int connectorStateVersion,
            String capabilityDigest) {
        if (mode != selection.getMode()
                || protocolVersion != selection.getRuntimeProtocolVersion()
                || connectorStateVersion != selection.getConnectorStateVersion()
                || !capabilityDigest.equals(selection.getCapabilityDigest())) {
            throw new IllegalStateException(
                    "Managed Source coordinator restore capability or lane mismatch");
        }
    }

    private void checkOwner() {
        long current = Thread.currentThread().getId();
        if (ownerThreadId == null) {
            ownerThreadId = current;
        } else if (ownerThreadId != current) {
            throw new IllegalStateException(
                    "Managed Source coordinator state accessed outside its event loop");
        }
    }

    private SourceCommandAdmissionAck ack(
            SourceCommandAdmissionStatus status,
            SourceCommandEnvelope command,
            long expectedSequence,
            String detail) {
        metrics.recordAdmission(status);
        return SourceCommandAdmissionAck.of(
                status, command, expectedSequence, config.getRetryInitialBackoffMillis(), detail);
    }

    private static long checksum(byte[] payload) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(payload);
        return crc.getValue();
    }

    private static long jitterBackoff(long lowerBound, long upperBound) {
        if (upperBound <= lowerBound) {
            return upperBound;
        }
        if (upperBound == Long.MAX_VALUE) {
            long candidate = ThreadLocalRandom.current().nextLong(lowerBound, upperBound);
            return ThreadLocalRandom.current().nextBoolean() ? candidate : upperBound;
        }
        return ThreadLocalRandom.current().nextLong(lowerBound, upperBound + 1L);
    }

    private SourceCommandAdmissionStatus registrationStatus(SourceCommandAdmissionStatus status) {
        metrics.recordAdmission(status);
        return status;
    }

    private void scheduleRegistrationDeadline(ReaderRegistration reader) {
        int subtask = reader.location.getTaskIndex();
        if (registrationDeadlineTimers.containsKey(subtask)) {
            return;
        }
        Cancellable deadline =
                scheduler.scheduleInCoordinatorThread(
                        AsyncTaskKey.of("reader-registration-deadline-" + subtask),
                        Duration.ofMillis(config.getCommandRetryDeadlineMillis()),
                        () -> {
                            registrationDeadlineTimers.remove(subtask);
                            ReaderRegistration current = readers.get(subtask);
                            if (current != null
                                    && current.attemptId.equals(reader.attemptId)
                                    && !current.ready) {
                                reportAsynchronousFailure(
                                        new IllegalStateException(
                                                "Managed Source Reader attempt was not replaced before the registration deadline: subtask="
                                                        + subtask));
                            }
                        });
        registrationDeadlineTimers.put(subtask, deadline);
    }

    private void removeInboundMailboxAccounting(ReaderCommandMailbox mailbox) {
        inboundMailboxCommands -= mailbox.size();
        inboundMailboxBytes -= mailbox.bytes();
        inboundReservedCommands -= mailbox.reservedCommands();
        inboundReservedBytes -= mailbox.reservedBytes();
        checkInboundAccounting();
    }

    private void checkInboundAccounting() {
        if (inboundMailboxCommands < 0
                || inboundMailboxBytes < 0
                || inboundReservedCommands < 0
                || inboundReservedBytes < 0) {
            throw new IllegalStateException(
                    "Managed Source aggregate inbound accounting underflow");
        }
    }

    private void clearCheckpointReportsForReader(String readerAttemptId) {
        checkpointReports.values().stream()
                .filter(accumulator -> accumulator.readerAttemptId.equals(readerAttemptId))
                .forEach(CheckpointReportAccumulator::close);
        checkpointReports
                .entrySet()
                .removeIf(entry -> entry.getValue().readerAttemptId.equals(readerAttemptId));
    }

    private void clearCheckpointReportsThrough(long checkpointId) {
        checkpointReports.values().stream()
                .filter(accumulator -> accumulator.checkpointId <= checkpointId)
                .forEach(CheckpointReportAccumulator::close);
        checkpointReports
                .entrySet()
                .removeIf(entry -> entry.getValue().checkpointId <= checkpointId);
    }

    private void updateMetrics() {
        int controlCommandCount;
        int reservedControlCommandCount;
        long currentInboundBytes;
        long currentInboundReservedBytes;
        long oldestAgeNanos;
        synchronized (admissionLock) {
            controlCommandCount = controlEvents.size();
            reservedControlCommandCount = controlEvents.size() - normalControlEvents;
            currentInboundBytes = inboundMailboxBytes;
            currentInboundReservedBytes = inboundReservedBytes;
            long nowNanos = System.nanoTime();
            if (lastOldestInboundSampleNanos == 0L
                    || nowNanos - lastOldestInboundSampleNanos
                            >= java.util.concurrent.TimeUnit.SECONDS.toNanos(1L)) {
                cachedOldestInboundAgeNanos =
                        inboundMailboxes.values().stream()
                                .mapToLong(mailbox -> mailbox.oldestCommandAgeNanos(nowNanos))
                                .max()
                                .orElse(0L);
                lastOldestInboundSampleNanos = nowNanos;
            }
            oldestAgeNanos = cachedOldestInboundAgeNanos;
        }
        metrics.updateCoordinatorState(
                controlCommandCount,
                currentInboundBytes,
                oldestAgeNanos,
                reservedControlCommandCount,
                currentInboundReservedBytes,
                outboundCommands,
                outboundBytes,
                assignmentTracker,
                scheduler);
    }

    private static final class ReaderRegistration {
        private final TaskLocation location;
        private final Address address;
        /** Monotonic task deployment identity used to fence delayed registration attempts. */
        private final long executionId;

        private final String attemptId;
        private final long restoredAppliedWatermark;
        private final long restoredNoMoreSplitsGeneration;
        private final Queue<SourceCommandEnvelope> pendingOutbound = new ArrayDeque<>();
        private boolean ready;
        private boolean epochApplied;
        private String epochCommandId = "";
        private SourceCommandEnvelope inFlightCommand;
        private int outboundCommands;
        private long outboundBytes;
        private int normalOutboundCommands;
        private long normalOutboundBytes;
        private long noMoreSplitsGenerationSent;

        private ReaderRegistration(
                TaskLocation location,
                Address address,
                long executionId,
                String attemptId,
                long restoredAppliedWatermark,
                long restoredNoMoreSplitsGeneration) {
            this.location = location;
            this.address = address;
            this.executionId = executionId;
            this.attemptId = attemptId;
            this.restoredAppliedWatermark = restoredAppliedWatermark;
            this.restoredNoMoreSplitsGeneration = restoredNoMoreSplitsGeneration;
        }
    }

    private static final class RegistrationFence {
        private final long executionId;
        private final String attemptId;

        private RegistrationFence(long executionId, String attemptId) {
            this.executionId = executionId;
            this.attemptId = attemptId;
        }
    }

    private static final class SerializedSplit {
        private final String splitId;
        private final byte[] payload;

        private SerializedSplit(String splitId, byte[] payload) {
            this.splitId = splitId;
            this.payload = payload;
        }
    }

    private static final class ControlEvent {
        private final CheckedRunnable action;

        @SuppressWarnings("unused")
        private final String description;

        private final boolean reserved;
        private final Runnable discardAction;

        private ControlEvent(CheckedRunnable action, String description, boolean reserved) {
            this(action, description, reserved, () -> {});
        }

        private ControlEvent(
                CheckedRunnable action,
                String description,
                boolean reserved,
                Runnable discardAction) {
            this.action = action;
            this.description = description;
            this.reserved = reserved;
            this.discardAction = discardAction;
        }

        private void discard() {
            discardAction.run();
        }
    }

    private static final class CheckpointReportAccumulator implements AutoCloseable {
        private final String readerAttemptId;
        private final long checkpointId;
        private final long appliedWatermark;
        private final SplitIdChunkAccumulator splitIds;

        private CheckpointReportAccumulator(
                String readerAttemptId,
                String groupId,
                int chunkCount,
                long checkpointId,
                long appliedWatermark,
                long maxBytes,
                ManagedSourceMemoryBudget workerBudget) {
            this.readerAttemptId = readerAttemptId;
            this.checkpointId = checkpointId;
            this.appliedWatermark = appliedWatermark;
            this.splitIds =
                    new SplitIdChunkAccumulator(groupId, chunkCount, maxBytes, workerBudget);
        }

        private void add(
                String currentGroupId,
                int currentChunkCount,
                long currentCheckpointId,
                long currentAppliedWatermark,
                int chunkIndex,
                List<String> splitIds) {
            if (currentCheckpointId != checkpointId
                    || currentAppliedWatermark != appliedWatermark) {
                throw new IllegalStateException(
                        "Inconsistent managed Source checkpoint report chunks");
            }
            this.splitIds.add(currentGroupId, currentChunkCount, chunkIndex, splitIds);
        }

        private boolean complete() {
            return splitIds.complete();
        }

        private Set<String> splitIds() {
            return splitIds.splitIds();
        }

        @Override
        public void close() {
            splitIds.close();
        }
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws Exception;
    }
}
