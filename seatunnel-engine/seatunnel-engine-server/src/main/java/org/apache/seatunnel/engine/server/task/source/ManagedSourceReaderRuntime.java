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
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.managed.ManagedSourceReader;
import org.apache.seatunnel.api.source.managed.PollStatus;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.engine.common.config.server.ManagedSourceRuntimeConfig;
import org.apache.seatunnel.engine.common.runtime.source.ManagedSourceRuntimeSelection;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelSourceCollector;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.operation.source.ManagedCoordinatorCommandOperation;
import org.apache.seatunnel.engine.server.task.operation.source.ManagedSourceRegisterOperation;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Single-owner event loop for a managed Source reader.
 *
 * <p>The existing Source task thread calls {@link #runOneTurn()}; no per-reader thread is created.
 * Hazelcast operation, timer, and completion threads only perform bounded admission or set terminal
 * signals. The watchdog may call the explicitly thread-safe {@link ManagedSourceReader#wakeUp()}
 * contract. All other connector callbacks and checkpoint-visible state are exclusively owned by the
 * task thread.
 */
@Slf4j
public final class ManagedSourceReaderRuntime<T, SplitT extends SourceSplit>
        implements AutoCloseable {
    private static final int MAX_CHECKPOINT_HISTORY = 128;
    private static final String SCHEMA_CHANGE_BEFORE = "SCHEMA-CHANGE-BEFORE";
    private static final String SCHEMA_CHANGE_AFTER = "SCHEMA-CHANGE-AFTER";

    private final SeaTunnelTask task;
    private final SourceAction<T, SplitT, ?> sourceAction;
    private final TaskLocation readerLocation;
    private final TaskLocation coordinatorLocation;
    private final Address coordinatorAddress;
    private final ManagedSourceReader<T, SplitT> reader;
    private final Serializer<SplitT> splitSerializer;
    private final SeaTunnelSourceCollector<T> collector;
    private final ManagedSourceRuntimeConfig config;
    private final ManagedSourceRuntimeSelection selection;
    private final ManagedSourceMemoryBudget outboundMemoryBudget;
    private final ManagedSourceRuntimeMetrics metrics;
    private final BudgetedCollector<T> budgetedCollector;
    private final long schemaChangeTimeoutMillis;
    private final String readerAttemptId = UUID.randomUUID().toString();
    private final ReaderCommandMailbox inboundMailbox;
    private final ManagedSourceLifecycle lifecycle = new ManagedSourceLifecycle();
    private final Queue<ControlEvent> orderedEvents = new ArrayDeque<>();
    private final Queue<OutboundCommand> pendingOutbound = new ArrayDeque<>();
    private final AtomicReference<Throwable> asynchronousFailure = new AtomicReference<>();
    private final AtomicBoolean terminalSignal = new AtomicBoolean();
    private final AtomicBoolean availabilitySignalled = new AtomicBoolean(true);
    private final Object admissionLock = new Object();
    private final Object pollWatchdogLock = new Object();
    private final Set<Long> completedCheckpoints = new HashSet<>();
    private final Set<Long> abortedCheckpoints = new HashSet<>();
    private final Set<String> uncheckpointedAssignmentSplitIds = new LinkedHashSet<>();
    private final Map<Long, Set<String>> checkpointAssignmentProofs = new HashMap<>();
    private final List<byte[]> stagedRestoredSplitStates = new ArrayList<>();
    private final Set<String> stagedRestoredAssignmentSplitIds = new LinkedHashSet<>();

    private String coordinatorEpoch = "";
    private String coordinatorAttemptId = "";
    private long appliedCommandWatermark;
    private final SortedSet<Long> appliedCommandGaps = new TreeSet<>();
    private long noMoreSplitsGeneration;
    private long nextOutboundSequence = 1L;
    private long registrationFirstAttemptNanos;
    private int registrationAttempts;
    private OutboundCommand inFlightOutbound;
    private int outboundCommands;
    private long outboundBytes;
    private int normalOutboundCommands;
    private long normalOutboundBytes;
    private int normalOrderedEvents;
    private volatile Thread ownerThread;
    private ScheduledFuture<?> pollWatchdogFuture;
    private long nextPollGeneration;
    private long activePollGeneration;
    private long activePollStartedNanos;
    private long wakeupPollGeneration;
    private long cancelledPollGeneration;
    private boolean registrationAccepted;
    private boolean closed;

    public ManagedSourceReaderRuntime(
            SeaTunnelTask task,
            SourceAction<T, SplitT, ?> sourceAction,
            TaskLocation readerLocation,
            TaskLocation coordinatorLocation,
            Address coordinatorAddress,
            ManagedSourceReader<T, SplitT> reader,
            Serializer<SplitT> splitSerializer,
            SeaTunnelSourceCollector<T> collector,
            ManagedSourceRuntimeConfig config,
            ManagedSourceRuntimeSelection selection) {
        this.task = task;
        this.sourceAction = sourceAction;
        this.readerLocation = readerLocation;
        this.coordinatorLocation = coordinatorLocation;
        this.coordinatorAddress = coordinatorAddress;
        this.reader = reader;
        this.splitSerializer = splitSerializer;
        this.collector = collector;
        this.config = config;
        this.selection = selection;
        this.metrics =
                new ManagedSourceRuntimeMetrics(
                        task.getMetricsContext(),
                        sourceAction.getId(),
                        task.getExecutionContext().getExecutionId());
        this.budgetedCollector = new BudgetedCollector<>(collector, metrics);
        this.outboundMemoryBudget =
                task.getExecutionContext().getTaskExecutionService().getManagedSourceMemoryBudget();
        this.schemaChangeTimeoutMillis =
                task.getExecutionContext()
                        .getTaskExecutionService()
                        .getSeaTunnelConfig()
                        .getEngineConfig()
                        .getCheckpointConfig()
                        .getSchemaChangeCheckpointTimeout();
        this.inboundMailbox = new ReaderCommandMailbox(config, outboundMemoryBudget, 1L);
        lifecycle.startRestore();
    }

    /** Starts the non-blocking registration handshake after the connector reader is open. */
    public void start() {
        if (closed || registrationAccepted) {
            return;
        }
        startPollWatchdog();
        registrationFirstAttemptNanos = System.nanoTime();
        invokeRegistration();
    }

    /** Restores engine metadata before the new attempt is registered. */
    public void restoreMetadata(ManagedReaderCheckpointState restored) {
        restoreMetadata(Collections.singletonList(restored));
    }

    /** Restores one or more old subtasks conservatively during rescale. */
    public void restoreMetadata(List<ManagedReaderCheckpointState> restoredStates) {
        if (restoredStates.isEmpty()) {
            return;
        }
        restoredStates.forEach(this::validateRestoredSelection);
        ManagedReaderCheckpointState first = restoredStates.get(0);
        lifecycle.restoreSnapshot(first.getLifecycleSnapshot());
        appliedCommandGaps.clear();
        if (restoredStates.size() == 1) {
            appliedCommandWatermark = first.getAppliedCommandWatermark();
            appliedCommandGaps.addAll(first.getAppliedCommandGaps());
        } else {
            // Old sender channels cannot be merged into one contiguous watermark.
            appliedCommandWatermark = 0L;
        }
        noMoreSplitsGeneration =
                restoredStates.stream()
                        .mapToLong(ManagedReaderCheckpointState::getNoMoreSplitsGeneration)
                        .max()
                        .orElse(0L);
        stagedRestoredSplitStates.clear();
        stagedRestoredAssignmentSplitIds.clear();
        for (ManagedReaderCheckpointState restored : restoredStates) {
            stagedRestoredSplitStates.addAll(restored.getConnectorSplitStates());
            stagedRestoredAssignmentSplitIds.addAll(restored.getCheckpointOwnedSplitIds());
        }
    }

    /** Performs bounded admission only; connector payloads are decoded by the owner thread. */
    public SourceCommandAdmissionAck admitCommand(SourceCommandEnvelope command) {
        long startedNanos = System.nanoTime();
        try {
            return admitCommandInternal(command);
        } finally {
            metrics.recordAdmissionDuration(
                    System.nanoTime() - startedNanos,
                    Duration.ofMillis(config.getAdmissionBudgetMillis()).toNanos());
        }
    }

    private SourceCommandAdmissionAck admitCommandInternal(SourceCommandEnvelope command) {
        if (closed || terminalSignal.get()) {
            return ack(
                    SourceCommandAdmissionStatus.TERMINAL_REJECTED,
                    command,
                    "Managed Source reader is closing");
        }
        if (command.getProtocolVersion() != selection.getRuntimeProtocolVersion()) {
            return ack(
                    SourceCommandAdmissionStatus.UNSUPPORTED_PROTOCOL,
                    command,
                    "Managed Source reader protocol mismatch");
        }
        if (command.getJobId() != readerLocation.getJobId()
                || command.getSourceRuntimeId() != coordinatorLocation.getTaskID()
                || !readerAttemptId.equals(command.getTargetAttemptId())) {
            return ack(
                    SourceCommandAdmissionStatus.STALE_TARGET,
                    command,
                    "Managed Source reader attempt mismatch");
        }
        boolean epochStart = command.getKind() == SourceCommandKind.READER_EPOCH_START;
        if (!epochStart
                && (!coordinatorEpoch.equals(command.getCoordinatorEpoch())
                        || !coordinatorAttemptId.equals(command.getSenderAttemptId()))) {
            return ack(
                    SourceCommandAdmissionStatus.STALE_TARGET,
                    command,
                    "Managed Source coordinator epoch mismatch");
        }
        if (!command.hasValidChecksum()
                || command.getPayloadSize() > config.getMaxCommandPayloadBytes()) {
            return ack(
                    SourceCommandAdmissionStatus.INVALID_PAYLOAD,
                    command,
                    "Managed Source command payload is invalid");
        }

        synchronized (admissionLock) {
            boolean reserved = command.usesReservedCapacity();
            if (!hasLocalCapacity(reserved)) {
                return ack(
                        reserved
                                ? SourceCommandAdmissionStatus.TERMINAL_REJECTED
                                : SourceCommandAdmissionStatus.RETRY_LATER,
                        command,
                        "Managed Source reader control queue is full");
            }
            SourceCommandAdmissionAck admission = inboundMailbox.offer(command);
            metrics.recordAdmission(admission.getStatus());
            if (admission.getStatus() == SourceCommandAdmissionStatus.ACCEPTED) {
                orderedEvents.add(
                        new ControlEvent(
                                this::applyNextInboundCommand,
                                "source-command-" + command.getCommandId(),
                                reserved));
                if (!reserved) {
                    normalOrderedEvents++;
                }
            }
            return admission;
        }
    }

    /** Admits a legacy coordinator callback when only the reader side is managed. */
    public void admitLegacySplits(List<SplitT> splits) {
        requireLocalAdmission(
                () -> applySplits(splits, "", Collections.emptyList()), "legacy-split-assignment");
    }

    /** Defers legacy split deserialization to the event-loop owner. */
    public void admitSerializedLegacySplits(List<byte[]> serializedSplits) {
        if (serializedSplits == null) {
            throw new IllegalArgumentException(
                    "Managed Source serialized split payloads must not be null");
        }
        List<byte[]> owned = new ArrayList<>(serializedSplits.size());
        long totalBytes = 0L;
        for (byte[] serializedSplit : serializedSplits) {
            if (serializedSplit == null) {
                throw new IllegalArgumentException(
                        "Managed Source serialized split payload must not be null");
            }
            totalBytes = Math.addExact(totalBytes, serializedSplit.length);
            if (totalBytes
                    > config.getReaderMailboxMaxBytes() - config.getReaderReservedControlBytes()) {
                throw new IllegalArgumentException(
                        "Managed Source serialized splits exceed normal mailbox byte capacity");
            }
            owned.add(serializedSplit);
        }
        List<byte[]> immutableOwned = Collections.unmodifiableList(owned);
        if (!outboundMemoryBudget.tryReserve(totalBytes)) {
            throw new IllegalStateException(
                    "Managed Source worker memory budget is exhausted during split admission");
        }
        long reservedBytes = totalBytes;
        boolean admitted =
                admitLocal(
                        () -> {
                            try {
                                List<SplitT> splits = new ArrayList<>(immutableOwned.size());
                                for (byte[] serialized : immutableOwned) {
                                    splits.add(splitSerializer.deserialize(serialized));
                                }
                                applySplits(splits, "", Collections.emptyList());
                            } finally {
                                outboundMemoryBudget.release(reservedBytes);
                            }
                        },
                        "legacy-serialized-split-assignment",
                        false,
                        () -> outboundMemoryBudget.release(reservedBytes));
        if (!admitted) {
            outboundMemoryBudget.release(reservedBytes);
            asynchronousFailure.compareAndSet(
                    null,
                    new IllegalStateException(
                            "Managed Source reader control mailbox exhausted: "
                                    + "legacy-serialized-split-assignment"));
        }
    }

    /** Admits a local barrier into the same ordered domain as remote commands. */
    public void admitBarrier(Barrier barrier) {
        requireLocalAdmission(() -> applyBarrier(barrier), "barrier-" + barrier.getId(), true);
    }

    public void admitCheckpointComplete(long checkpointId) {
        requireLocalAdmission(
                () -> applyCheckpointComplete(checkpointId),
                "checkpoint-complete-" + checkpointId,
                true);
    }

    public void admitCheckpointAborted(long checkpointId) {
        requireLocalAdmission(
                () -> applyCheckpointAborted(checkpointId),
                "checkpoint-aborted-" + checkpointId,
                true);
    }

    public void admitCheckpointEnd(long checkpointId) {
        requireLocalAdmission(
                () -> applyCheckpointEnd(checkpointId), "checkpoint-end-" + checkpointId, true);
    }

    /** Serializes timer-driven flush signals with records and barriers. */
    public void admitFlushSignal(long jobId, long taskId) {
        requireLocalAdmission(
                () -> collector.sendFlushSignal(jobId, taskId), "source-flush-signal");
    }

    /** Executes one ordered callback or one cooperative poll turn. */
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
            lifecycle.fail(failure);
        }
        if (terminalSignal.get()) {
            lifecycle.cancel();
        }
        if (lifecycle.isFailed()) {
            throw new IllegalStateException(
                    "Managed Source reader runtime failed", lifecycle.getFailure());
        }

        lifecycle.checkSchemaTimeout(
                System.nanoTime(), Duration.ofMillis(schemaChangeTimeoutMillis).toNanos());
        if (lifecycle.isFailed()) {
            throw new IllegalStateException(
                    "Managed Source reader runtime failed", lifecycle.getFailure());
        }

        ControlEvent event;
        synchronized (admissionLock) {
            event = pollNextReadyEvent();
            if (event != null && !event.reserved) {
                normalOrderedEvents--;
            }
        }
        if (event != null) {
            event.action.run();
            return true;
        }
        if (!lifecycle.canPoll() || !availabilitySignalled.getAndSet(false)) {
            return false;
        }
        PollStatus status = pollManagedReader();
        handleSchemaSignals();
        switch (status) {
            case MORE_AVAILABLE:
                availabilitySignalled.set(true);
                return true;
            case NOTHING_AVAILABLE:
                subscribeToAvailability();
                return false;
            case END_OF_INPUT:
                signalNoMoreElement();
                return true;
            default:
                throw new IllegalArgumentException("Unknown managed Source poll status " + status);
        }
    }

    public void requestSplit() {
        sendCoordinatorCommand(
                SourceCommandKind.REQUEST_SPLIT,
                SourceCommandDurability.RECONSTRUCTABLE,
                SourceCommandCodec.EMPTY_CODEC,
                "",
                0,
                1,
                new byte[0],
                () -> {});
    }

    public void sendSourceEvent(SourceEvent event) {
        throw new UnsupportedOperationException(
                "Managed Source custom events require a versioned event codec, which protocol version 1 does not expose");
    }

    public void signalNoMoreElement() {
        if (lifecycle.isDraining()) {
            return;
        }
        lifecycle.gracefulClose();
        sendCoordinatorCommand(
                SourceCommandKind.READER_FINISHED,
                SourceCommandDurability.RECONSTRUCTABLE,
                SourceCommandCodec.EMPTY_CODEC,
                "",
                0,
                1,
                new byte[0],
                () -> {});
    }

    public boolean isReady() {
        return registrationAccepted
                && lifecycle.getMainState() == ManagedSourceLifecycleState.RUNNING;
    }

    public String getReaderAttemptId() {
        return readerAttemptId;
    }

    public long getAppliedCommandWatermark() {
        return appliedCommandWatermark;
    }

    /** Fails the runtime from an asynchronous engine callback and wakes the event-loop owner. */
    public void reportAsynchronousFailure(Throwable failure) {
        asynchronousFailure.compareAndSet(null, failure);
        inboundMailbox.signal();
        try {
            reader.wakeUp();
        } catch (Throwable wakeupFailure) {
            failure.addSuppressed(wakeupFailure);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        terminalSignal.set(true);
        if (pollWatchdogFuture != null) {
            pollWatchdogFuture.cancel(false);
            pollWatchdogFuture = null;
        }
        try {
            reader.wakeUp();
        } catch (Throwable t) {
            log.warn("Failed to wake managed Source reader during close", t);
        }
        synchronized (admissionLock) {
            inboundMailbox.close();
            orderedEvents.forEach(ControlEvent::discard);
            orderedEvents.clear();
            normalOrderedEvents = 0;
        }
        releaseAllOutbound();
        pendingOutbound.clear();
        inFlightOutbound = null;
        completedCheckpoints.clear();
        abortedCheckpoints.clear();
        checkpointAssignmentProofs.clear();
        uncheckpointedAssignmentSplitIds.clear();
        stagedRestoredSplitStates.clear();
        stagedRestoredAssignmentSplitIds.clear();
        lifecycle.closed();
    }

    private PollStatus pollManagedReader() throws Exception {
        long startedNanos = System.nanoTime();
        long pollGeneration;
        synchronized (pollWatchdogLock) {
            pollGeneration = ++nextPollGeneration;
            activePollGeneration = pollGeneration;
            activePollStartedNanos = startedNanos;
        }
        ManagedPollContext pollContext =
                new ManagedPollContext(
                        config.getPollMaxRecords(),
                        config.getPollMaxBytes(),
                        startedNanos
                                + Duration.ofMillis(config.getPollSoftDurationMillis()).toNanos());
        collector.resetEmptyThisPollNext();
        budgetedCollector.beginPoll(pollContext);
        try {
            PollStatus status = reader.pollNextManaged(budgetedCollector, pollContext);
            if (status == null) {
                throw new IllegalStateException(
                        "Managed Source reader returned a null poll status");
            }
            return status;
        } finally {
            budgetedCollector.endPoll(pollContext);
            synchronized (pollWatchdogLock) {
                if (activePollGeneration == pollGeneration) {
                    activePollGeneration = 0L;
                    activePollStartedNanos = 0L;
                }
            }
            long elapsedNanos = System.nanoTime() - startedNanos;
            long elapsedMillis = Duration.ofNanos(elapsedNanos).toMillis();
            metrics.recordPoll(
                    elapsedNanos,
                    pollContext.emittedRecords(),
                    pollContext.emittedBytes(),
                    elapsedNanos > Duration.ofMillis(config.getPollSoftDurationMillis()).toNanos());
            if (elapsedMillis >= config.getPollSoftDurationMillis()) {
                log.warn(
                        "Managed Source poll exceeded soft budget: source={}, task={}, elapsed={}ms, hardExceeded={}",
                        sourceAction.getName(),
                        readerLocation,
                        elapsedMillis,
                        elapsedMillis >= config.getPollHardDurationMillis());
            }
        }
    }

    private void startPollWatchdog() {
        if (pollWatchdogFuture != null) {
            return;
        }
        long checkIntervalMillis =
                Math.max(1L, Math.min(100L, config.getPollHardDurationMillis() / 4L));
        pollWatchdogFuture =
                task.getExecutionContext()
                        .getTaskExecutionService()
                        .scheduleManagedSourcePollWatchdog(
                                this::checkActivePoll, checkIntervalMillis);
    }

    private void checkActivePoll() {
        boolean wakeUp = false;
        Thread threadToInterrupt = null;
        synchronized (pollWatchdogLock) {
            if (closed || activePollGeneration == 0L || activePollStartedNanos == 0L) {
                return;
            }
            long elapsedNanos = Math.max(0L, System.nanoTime() - activePollStartedNanos);
            long hardBudgetNanos = Duration.ofMillis(config.getPollHardDurationMillis()).toNanos();
            if (elapsedNanos >= hardBudgetNanos && wakeupPollGeneration != activePollGeneration) {
                wakeupPollGeneration = activePollGeneration;
                metrics.recordWakeup();
                wakeUp = true;
            }
            long cancellationNanos =
                    Duration.ofMillis(config.getPollCancellationTimeoutMillis()).toNanos();
            if (elapsedNanos >= hardBudgetNanos
                    && elapsedNanos - hardBudgetNanos >= cancellationNanos
                    && cancelledPollGeneration != activePollGeneration) {
                cancelledPollGeneration = activePollGeneration;
                metrics.recordWakeupTimeout();
                asynchronousFailure.compareAndSet(
                        null,
                        new IllegalStateException(
                                "Managed Source poll exceeded cancellation timeout"));
                threadToInterrupt = ownerThread;
            }
        }
        if (wakeUp) {
            try {
                reader.wakeUp();
            } catch (Throwable wakeupFailure) {
                asynchronousFailure.compareAndSet(null, wakeupFailure);
            }
        }
        if (threadToInterrupt != null) {
            threadToInterrupt.interrupt();
        }
    }

    private void subscribeToAvailability() {
        // Declared as CompletionStage rather than CompletableFuture: the runtime only observes the
        // future the connector returns, and engine code must not import
        // java.util.concurrent.CompletableFuture (enforced by ImportClassCheckTest).
        CompletionStage<Void> available;
        try {
            available = reader.isAvailable();
        } catch (Throwable t) {
            asynchronousFailure.compareAndSet(null, t);
            return;
        }
        if (available == null) {
            asynchronousFailure.compareAndSet(
                    null,
                    new IllegalStateException(
                            "Managed Source reader returned a null availability future"));
            return;
        }
        available.whenComplete(
                (ignored, failure) -> {
                    if (failure != null) {
                        asynchronousFailure.compareAndSet(null, failure);
                    }
                    availabilitySignalled.set(true);
                });
    }

    private void applyNextInboundCommand() throws Exception {
        SourceCommandEnvelope command;
        synchronized (admissionLock) {
            command = inboundMailbox.pollNext();
        }
        if (command == null) {
            throw new IllegalStateException("Managed Source reader command sequence has a gap");
        }
        long startedNanos = System.nanoTime();
        try {
            applyInboundCommand(command);
            appliedCommandWatermark = command.getSenderSequence();
            appliedCommandGaps.remove(command.getSenderSequence());
        } finally {
            metrics.recordCommand(
                    command.getAdmittedNanos() <= 0
                            ? 0L
                            : Math.max(0L, startedNanos - command.getAdmittedNanos()),
                    Math.max(0L, System.nanoTime() - startedNanos));
        }
    }

    private void applyInboundCommand(SourceCommandEnvelope command) throws Exception {
        validateCodec(command);
        switch (command.getKind()) {
            case READER_EPOCH_START:
                applyEpochStart(command);
                break;
            case ASSIGN_SPLITS:
                SourceCommandCodec.SplitAssignment assignment =
                        SourceCommandCodec.decodeSplitAssignment(command.getPayload());
                List<SplitT> splits = deserializeSplits(assignment);
                applySplits(splits, command.getCommandId(), assignment.getSplitIds());
                break;
            case NO_MORE_SPLITS:
                long generation = SourceCommandCodec.decodeNoMoreSplits(command.getPayload());
                if (generation > noMoreSplitsGeneration) {
                    reader.handleNoMoreSplits();
                    noMoreSplitsGeneration = generation;
                }
                sendCommandApplied(command.getCommandId(), Collections.emptyList());
                break;
            case SOURCE_EVENT:
                throw new UnsupportedOperationException(
                        "Managed Source protocol version 1 does not support custom Source events");
            case BARRIER:
                applyBarrier(SourceCommandCodec.decodeBarrier(command.getPayload()));
                break;
            case CHECKPOINT_COMPLETE:
                applyCheckpointComplete(
                        SourceCommandCodec.decodeCheckpointId(command.getPayload()));
                break;
            case CHECKPOINT_ABORTED:
                applyCheckpointAborted(SourceCommandCodec.decodeCheckpointId(command.getPayload()));
                break;
            case CHECKPOINT_END:
                applyCheckpointEnd(SourceCommandCodec.decodeCheckpointId(command.getPayload()));
                break;
            case PREPARE_CLOSE:
                lifecycle.gracefulClose();
                break;
            case CANCEL:
                terminalSignal.set(true);
                lifecycle.cancel();
                reader.wakeUp();
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported coordinator-to-reader Source command " + command.getKind());
        }
    }

    private void applyEpochStart(SourceCommandEnvelope command) {
        if (!coordinatorEpoch.isEmpty()
                && (!coordinatorEpoch.equals(command.getCoordinatorEpoch())
                        || !coordinatorAttemptId.equals(command.getSenderAttemptId()))) {
            throw new IllegalStateException(
                    "Managed Source reader received a second coordinator epoch");
        }
        coordinatorEpoch = command.getCoordinatorEpoch();
        coordinatorAttemptId = command.getSenderAttemptId();
        registrationAccepted = true;
        if (lifecycle.getMainState() == ManagedSourceLifecycleState.RESTORING) {
            lifecycle.finishRestore();
        }
        sendStagedRestoreCommands();
        sendCommandApplied(command.getCommandId(), Collections.emptyList());
    }

    private void applySplits(List<SplitT> splits, String commandId, List<String> splitIds) {
        reader.addSplits(splits);
        uncheckpointedAssignmentSplitIds.addAll(splitIds);
        if (!commandId.isEmpty()) {
            sendCommandApplied(commandId, splitIds);
        }
        availabilitySignalled.set(true);
    }

    private void applyBarrier(Barrier barrier) throws Exception {
        if (barrier.prepareClose(readerLocation)) {
            lifecycle.gracefulClose();
        }
        bindSchemaCheckpointIfNeeded(barrier);
        if (!barrier.snapshot()) {
            finishBarrier(barrier);
            return;
        }

        lifecycle.beginCheckpointBarrier(barrier.getId());
        try {
            List<SplitT> splitStates = reader.snapshotState(barrier.getId());
            List<byte[]> connectorStates = new ArrayList<>(splitStates.size());
            List<String> splitIds = new ArrayList<>(splitStates.size());
            for (SplitT split : splitStates) {
                connectorStates.add(splitSerializer.serialize(split));
                splitIds.add(split.splitId());
            }
            Set<String> checkpointOwnedSplitIds =
                    new LinkedHashSet<>(uncheckpointedAssignmentSplitIds);
            checkpointOwnedSplitIds.addAll(splitIds);
            checkpointAssignmentProofs.put(
                    barrier.getId(), new LinkedHashSet<>(checkpointOwnedSplitIds));
            ManagedReaderCheckpointState checkpointState =
                    new ManagedReaderCheckpointState(
                            selection.getMode(),
                            selection.getRuntimeProtocolVersion(),
                            selection.getConnectorStateVersion(),
                            selection.getCapabilityDigest(),
                            readerAttemptId,
                            coordinatorEpoch,
                            appliedCommandWatermark,
                            appliedCommandGaps,
                            noMoreSplitsGeneration,
                            lifecycle.snapshot(),
                            new ArrayList<>(checkpointOwnedSplitIds),
                            connectorStates);
            byte[] serialized = ManagedReaderCheckpointStateSerializer.serialize(checkpointState);
            task.addState(
                    barrier,
                    ActionStateKey.of(sourceAction),
                    Collections.singletonList(serialized));
            sendCheckpointReports(barrier, new ArrayList<>(checkpointOwnedSplitIds));
        } catch (Exception exception) {
            lifecycle.finishCheckpointBarrier();
            throw exception;
        } catch (Error error) {
            lifecycle.finishCheckpointBarrier();
            throw error;
        }
    }

    private void sendCheckpointReports(Barrier barrier, List<String> splitIds) {
        List<List<String>> chunks = chunkSplitIds(splitIds);
        String groupId = UUID.randomUUID().toString();
        int[] admittedChunks = {0};
        for (int chunkIndex = 0; chunkIndex < chunks.size(); chunkIndex++) {
            List<String> chunk = chunks.get(chunkIndex);
            byte[] payload =
                    SourceCommandCodec.encodeReaderCheckpointReport(
                            barrier.getId(), appliedCommandWatermark, chunk);
            sendCoordinatorCommand(
                    SourceCommandKind.READER_CHECKPOINT_REPORT,
                    SourceCommandDurability.CHECKPOINT_COUPLED,
                    SourceCommandCodec.READER_CHECKPOINT_REPORT_CODEC,
                    groupId,
                    chunkIndex,
                    chunks.size(),
                    payload,
                    () -> {
                        admittedChunks[0]++;
                        if (admittedChunks[0] == chunks.size()) {
                            finishBarrier(barrier);
                        }
                    });
        }
    }

    private List<List<String>> chunkSplitIds(List<String> splitIds) {
        if (splitIds.isEmpty()) {
            return Collections.singletonList(Collections.emptyList());
        }
        List<List<String>> chunks = new ArrayList<>();
        List<String> current = new ArrayList<>();
        int currentBytes = SourceCommandCodec.readerCheckpointReportBaseSize();
        for (String splitId : splitIds) {
            int entryBytes = SourceCommandCodec.readerCheckpointReportEntrySize(splitId);
            if (entryBytes
                    > config.getMaxCommandPayloadBytes()
                            - SourceCommandCodec.readerCheckpointReportBaseSize()) {
                throw new IllegalArgumentException(
                        "Managed Source split identifier exceeds command payload limit");
            }
            if (!current.isEmpty()
                    && (current.size() >= SourceCommandCodec.maxCollectionSize()
                            || entryBytes > config.getMaxCommandPayloadBytes() - currentBytes)) {
                chunks.add(current);
                current = new ArrayList<>();
                currentBytes = SourceCommandCodec.readerCheckpointReportBaseSize();
            }
            current.add(splitId);
            currentBytes = Math.addExact(currentBytes, entryBytes);
        }
        if (!current.isEmpty()) {
            chunks.add(current);
        }
        return chunks;
    }

    private void finishBarrier(Barrier barrier) throws IOException {
        task.ack(barrier);
        collector.sendRecordToNext(new Record<>(barrier));
        lifecycle.finishCheckpointBarrier();
    }

    private void applyCheckpointComplete(long checkpointId) throws Exception {
        if (abortedCheckpoints.contains(checkpointId) || !completedCheckpoints.add(checkpointId)) {
            return;
        }
        trimCheckpointHistory();
        reader.notifyCheckpointComplete(checkpointId);
        Set<String> includedAssignments = checkpointAssignmentProofs.remove(checkpointId);
        if (includedAssignments != null) {
            uncheckpointedAssignmentSplitIds.removeAll(includedAssignments);
        }
        checkpointAssignmentProofs.keySet().removeIf(id -> id < checkpointId);
    }

    private void applyCheckpointAborted(long checkpointId) throws Exception {
        if (completedCheckpoints.contains(checkpointId) || !abortedCheckpoints.add(checkpointId)) {
            return;
        }
        trimCheckpointHistory();
        reader.notifyCheckpointAborted(checkpointId);
        checkpointAssignmentProofs.remove(checkpointId);
        lifecycle.checkpointAborted(checkpointId);
    }

    private void applyCheckpointEnd(long checkpointId) {
        lifecycle.checkpointEnded(checkpointId);
    }

    private void trimCheckpointHistory() {
        while (completedCheckpoints.size() > MAX_CHECKPOINT_HISTORY) {
            completedCheckpoints.remove(Collections.min(completedCheckpoints));
        }
        while (abortedCheckpoints.size() > MAX_CHECKPOINT_HISTORY) {
            abortedCheckpoints.remove(Collections.min(abortedCheckpoints));
        }
    }

    private void handleSchemaSignals() {
        String phase = null;
        if (collector.captureSchemaChangeBeforeCheckpointSignal()) {
            phase = SCHEMA_CHANGE_BEFORE;
        } else if (collector.captureSchemaChangeAfterCheckpointSignal()) {
            phase = SCHEMA_CHANGE_AFTER;
        }
        if (phase == null) {
            return;
        }
        long requestEpoch = lifecycle.beginSchemaChange(phase, System.nanoTime());
        lifecycle.schemaTriggerRequested(requestEpoch);
        InvocationFuture<Object> future =
                SCHEMA_CHANGE_BEFORE.equals(phase)
                        ? task.triggerSchemaChangeBeforeCheckpoint()
                        : task.triggerSchemaChangeAfterCheckpoint();
        String requestedPhase = phase;
        future.whenComplete(
                (ignored, failure) ->
                        requireInternalAdmission(
                                () -> {
                                    if (failure != null
                                            && lifecycle.getSchemaRequestEpoch() == requestEpoch
                                            && lifecycle.getSchemaPhase().equals(requestedPhase)) {
                                        lifecycle.fail(failure);
                                    }
                                },
                                "schema-trigger-result-" + requestEpoch));
    }

    private void bindSchemaCheckpointIfNeeded(Barrier barrier) {
        if (!(barrier instanceof CheckpointBarrier)
                || lifecycle.getSchemaState() != SchemaChangeSubState.TRIGGER_REQUESTED) {
            return;
        }
        CheckpointType checkpointType = ((CheckpointBarrier) barrier).getCheckpointType();
        String phase = null;
        if (checkpointType.isSchemaChangeBeforeCheckpoint()) {
            phase = SCHEMA_CHANGE_BEFORE;
        } else if (checkpointType.isSchemaChangeAfterCheckpoint()) {
            phase = SCHEMA_CHANGE_AFTER;
        }
        if (phase != null
                && !lifecycle.bindSchemaCheckpoint(
                        phase, barrier.getId(), lifecycle.getSchemaRequestEpoch())) {
            throw new IllegalStateException(
                    "Managed Source schema checkpoint does not match the active request");
        }
    }

    private void sendCommandApplied(String commandId, List<String> splitIds) {
        sendCoordinatorCommand(
                SourceCommandKind.COMMAND_APPLIED,
                SourceCommandDurability.RECONSTRUCTABLE,
                SourceCommandCodec.COMMAND_APPLIED_CODEC,
                "",
                0,
                1,
                SourceCommandCodec.encodeCommandApplied(commandId, splitIds),
                () -> {});
    }

    /**
     * Replays completed-checkpoint split state before acknowledging the new coordinator epoch.
     *
     * <p>The Reader-to-coordinator channel is single-flight, so every restore chunk is admitted
     * before the epoch application command. The coordinator therefore cannot replay an old
     * assignment before applying the checkpoint ownership proof.
     */
    private void sendStagedRestoreCommands() {
        if (stagedRestoredSplitStates.isEmpty() && stagedRestoredAssignmentSplitIds.isEmpty()) {
            return;
        }
        List<byte[]> payloads = new ArrayList<>();
        appendRestoredStateChunks(payloads);
        appendRestoredProofChunks(payloads);
        String groupId = UUID.randomUUID().toString();
        for (int chunkIndex = 0; chunkIndex < payloads.size(); chunkIndex++) {
            sendCoordinatorCommand(
                    SourceCommandKind.RESTORED_SPLITS,
                    SourceCommandDurability.CHECKPOINT_COUPLED,
                    SourceCommandCodec.RESTORED_SPLITS_CODEC,
                    groupId,
                    chunkIndex,
                    payloads.size(),
                    payloads.get(chunkIndex),
                    () -> {});
        }
        stagedRestoredSplitStates.clear();
        stagedRestoredAssignmentSplitIds.clear();
    }

    /** Chunks connector split state so no restore command can exceed the production wire limit. */
    private void appendRestoredStateChunks(List<byte[]> payloads) {
        List<byte[]> current = new ArrayList<>();
        int currentBytes = SourceCommandCodec.restoredSplitsBaseSize();
        for (byte[] splitState : stagedRestoredSplitStates) {
            int entryBytes = SourceCommandCodec.restoredSplitStateEntrySize(splitState);
            if (entryBytes
                    > config.getMaxCommandPayloadBytes()
                            - SourceCommandCodec.restoredSplitsBaseSize()) {
                throw new IllegalArgumentException(
                        "Managed Source restored split exceeds command payload limit");
            }
            if (!current.isEmpty()
                    && (current.size() >= SourceCommandCodec.maxCollectionSize()
                            || entryBytes > config.getMaxCommandPayloadBytes() - currentBytes)) {
                payloads.add(
                        SourceCommandCodec.encodeRestoredSplits(current, Collections.emptyList()));
                current = new ArrayList<>();
                currentBytes = SourceCommandCodec.restoredSplitsBaseSize();
            }
            current.add(splitState);
            currentBytes = Math.addExact(currentBytes, entryBytes);
        }
        if (!current.isEmpty()) {
            payloads.add(SourceCommandCodec.encodeRestoredSplits(current, Collections.emptyList()));
        }
    }

    /** Chunks assignment proof identifiers independently from connector split state. */
    private void appendRestoredProofChunks(List<byte[]> payloads) {
        List<String> current = new ArrayList<>();
        int currentBytes = SourceCommandCodec.restoredSplitsBaseSize();
        for (String splitId : stagedRestoredAssignmentSplitIds) {
            int entryBytes = SourceCommandCodec.restoredSplitProofEntrySize(splitId);
            if (entryBytes
                    > config.getMaxCommandPayloadBytes()
                            - SourceCommandCodec.restoredSplitsBaseSize()) {
                throw new IllegalArgumentException(
                        "Managed Source restored split identifier exceeds command payload limit");
            }
            if (!current.isEmpty()
                    && (current.size() >= SourceCommandCodec.maxCollectionSize()
                            || entryBytes > config.getMaxCommandPayloadBytes() - currentBytes)) {
                payloads.add(
                        SourceCommandCodec.encodeRestoredSplits(Collections.emptyList(), current));
                current = new ArrayList<>();
                currentBytes = SourceCommandCodec.restoredSplitsBaseSize();
            }
            current.add(splitId);
            currentBytes = Math.addExact(currentBytes, entryBytes);
        }
        if (!current.isEmpty()) {
            payloads.add(SourceCommandCodec.encodeRestoredSplits(Collections.emptyList(), current));
        }
    }

    private void sendCoordinatorCommand(
            SourceCommandKind kind,
            SourceCommandDurability durability,
            int codecId,
            String groupId,
            int chunkIndex,
            int chunkCount,
            byte[] payload,
            CheckedRunnable acceptedCallback) {
        checkOwner();
        if (coordinatorEpoch.isEmpty() || coordinatorAttemptId.isEmpty()) {
            throw new IllegalStateException(
                    "Managed Source reader cannot send commands before epoch handshake");
        }
        if (payload.length > config.getMaxCommandPayloadBytes()) {
            throw new IllegalArgumentException("Managed Source command payload exceeds limit");
        }
        SourceCommandEnvelope envelope =
                SourceCommandEnvelope.create(
                        readerLocation.getJobId(),
                        coordinatorLocation.getTaskID(),
                        coordinatorEpoch,
                        readerAttemptId,
                        coordinatorAttemptId,
                        nextOutboundSequence++,
                        kind,
                        durability,
                        SourceCommandCodec.PAYLOAD_VERSION,
                        codecId,
                        groupId,
                        chunkIndex,
                        chunkCount,
                        payload);
        reserveOutbound(envelope);
        pendingOutbound.add(new OutboundCommand(envelope, acceptedCallback, System.nanoTime()));
        sendNextOutbound();
    }

    private void sendNextOutbound() {
        if (inFlightOutbound != null || closed) {
            return;
        }
        inFlightOutbound = pendingOutbound.poll();
        if (inFlightOutbound != null) {
            invokeOutbound(inFlightOutbound);
        }
    }

    private void invokeOutbound(OutboundCommand outbound) {
        InvocationFuture<?> future =
                task.getExecutionContext()
                        .sendToMember(
                                new ManagedCoordinatorCommandOperation(
                                        coordinatorLocation, outbound.command),
                                coordinatorAddress);
        future.whenComplete(
                (result, failure) ->
                        requireInternalAdmission(
                                () ->
                                        handleOutboundResult(
                                                outbound,
                                                result instanceof SourceCommandAdmissionAck
                                                        ? (SourceCommandAdmissionAck) result
                                                        : null,
                                                failure),
                                "reader-transport-result-" + outbound.command.getCommandId()));
    }

    private void handleOutboundResult(
            OutboundCommand outbound, SourceCommandAdmissionAck admission, Throwable failure)
            throws Exception {
        checkOwner();
        if (inFlightOutbound != outbound) {
            return;
        }
        if (failure != null || admission == null) {
            scheduleOutboundRetry(outbound, config.getRetryInitialBackoffMillis());
            return;
        }
        switch (admission.getStatus()) {
            case ACCEPTED:
            case DUPLICATE:
                try {
                    outbound.acceptedCallback.run();
                } finally {
                    releaseOutbound(outbound.command);
                    inFlightOutbound = null;
                }
                sendNextOutbound();
                return;
            case RETRY_LATER:
                scheduleOutboundRetry(
                        outbound,
                        Math.max(
                                config.getRetryInitialBackoffMillis(),
                                admission.getRetryAfterMillis()));
                return;
            case STALE_TARGET:
            case TERMINAL_REJECTED:
            case UNSUPPORTED_PROTOCOL:
            case INVALID_PAYLOAD:
                throw new IllegalStateException(
                        "Managed Source Reader-to-coordinator command "
                                + outbound.command.getCommandId()
                                + " rejected: "
                                + admission.getStatus()
                                + " "
                                + admission.getDetail());
            default:
                throw new IllegalArgumentException(
                        "Unknown managed Source admission " + admission.getStatus());
        }
    }

    private void scheduleOutboundRetry(OutboundCommand outbound, long suggestedBackoffMillis) {
        long elapsedMillis =
                Duration.ofNanos(System.nanoTime() - outbound.firstAttemptNanos).toMillis();
        if (elapsedMillis >= config.getCommandRetryDeadlineMillis()) {
            if (outbound.command.getDurability() == SourceCommandDurability.EPHEMERAL) {
                releaseOutbound(outbound.command);
                inFlightOutbound = null;
                sendNextOutbound();
                return;
            }
            throw new IllegalStateException(
                    "Managed Source Reader-to-coordinator command exceeded retry deadline");
        }
        metrics.recordTransportRetry();
        outbound.attempts++;
        long exponential =
                config.getRetryInitialBackoffMillis()
                        * (1L << Math.min(20, Math.max(0, outbound.attempts - 1)));
        long computedBackoff =
                Math.min(
                        config.getRetryMaxBackoffMillis(),
                        Math.max(suggestedBackoffMillis, exponential));
        long backoff =
                jitterBackoff(Math.min(computedBackoff, suggestedBackoffMillis), computedBackoff);
        task.getExecutionContext()
                .getTaskExecutionService()
                .scheduleManagedSourceCoordinatorTimer(
                        () ->
                                requireInternalAdmission(
                                        () -> {
                                            if (inFlightOutbound == outbound && !closed) {
                                                invokeOutbound(outbound);
                                            }
                                        },
                                        "reader-transport-retry-"
                                                + outbound.command.getCommandId()),
                        backoff);
    }

    private void reserveOutbound(SourceCommandEnvelope command) {
        int bytes = command.estimatedSizeBytes();
        boolean reserved = command.usesReservedCapacity();
        int normalMaxCommands =
                config.getReaderMailboxMaxCommands() - config.getReaderReservedControlCommands();
        long normalMaxBytes =
                config.getReaderMailboxMaxBytes() - config.getReaderReservedControlBytes();
        if (outboundCommands >= config.getReaderMailboxMaxCommands()
                || bytes > config.getReaderMailboxMaxBytes() - outboundBytes
                || (!reserved
                        && (normalOutboundCommands >= normalMaxCommands
                                || bytes > normalMaxBytes - normalOutboundBytes))) {
            throw new IllegalStateException(
                    "Managed Source Reader-to-coordinator outbound window is full");
        }
        if (!outboundMemoryBudget.tryReserve(bytes)) {
            throw new IllegalStateException(
                    "Managed Source worker outbound memory budget is exhausted");
        }
        outboundCommands++;
        outboundBytes += bytes;
        if (!reserved) {
            normalOutboundCommands++;
            normalOutboundBytes += bytes;
        }
    }

    private void releaseOutbound(SourceCommandEnvelope command) {
        int bytes = command.estimatedSizeBytes();
        outboundCommands--;
        outboundBytes -= bytes;
        if (!command.usesReservedCapacity()) {
            normalOutboundCommands--;
            normalOutboundBytes -= bytes;
        }
        if (outboundCommands < 0
                || outboundBytes < 0
                || normalOutboundCommands < 0
                || normalOutboundBytes < 0) {
            throw new IllegalStateException(
                    "Managed Source Reader outbound command accounting underflow");
        }
        outboundMemoryBudget.release(bytes);
    }

    private void releaseAllOutbound() {
        if (outboundBytes > 0) {
            outboundMemoryBudget.release(outboundBytes);
        }
        outboundCommands = 0;
        outboundBytes = 0L;
        normalOutboundCommands = 0;
        normalOutboundBytes = 0L;
    }

    private void invokeRegistration() {
        if (closed || registrationAccepted) {
            return;
        }
        registrationAttempts++;
        InvocationFuture<?> future =
                task.getExecutionContext()
                        .sendToMember(
                                new ManagedSourceRegisterOperation(
                                        coordinatorLocation,
                                        readerLocation,
                                        task.getExecutionContext().getExecutionId(),
                                        readerAttemptId,
                                        selection.getRuntimeProtocolVersion(),
                                        selection.getCapabilityDigest(),
                                        1L,
                                        appliedCommandWatermark,
                                        noMoreSplitsGeneration),
                                coordinatorAddress);
        future.whenComplete(
                (result, failure) ->
                        requireInternalAdmission(
                                () -> handleRegistrationResult(result, failure),
                                "managed-reader-registration-result"));
    }

    private void handleRegistrationResult(Object result, Throwable failure) {
        checkOwner();
        SourceCommandAdmissionStatus status =
                result instanceof Integer
                        ? SourceCommandAdmissionStatus.fromCode((Integer) result)
                        : null;
        if (failure == null
                && (status == SourceCommandAdmissionStatus.ACCEPTED
                        || status == SourceCommandAdmissionStatus.DUPLICATE)) {
            registrationAccepted = true;
            return;
        }
        if (status != null
                && status != SourceCommandAdmissionStatus.RETRY_LATER
                && status != SourceCommandAdmissionStatus.STALE_TARGET) {
            throw new IllegalStateException(
                    "Managed Source reader registration rejected: " + status);
        }
        long elapsedMillis =
                Duration.ofNanos(System.nanoTime() - registrationFirstAttemptNanos).toMillis();
        if (elapsedMillis >= config.getCommandRetryDeadlineMillis()) {
            throw new IllegalStateException(
                    "Managed Source reader registration exceeded retry deadline", failure);
        }
        metrics.recordTransportRetry();
        long exponential =
                config.getRetryInitialBackoffMillis()
                        * (1L << Math.min(20, Math.max(0, registrationAttempts - 1)));
        long computedBackoff = Math.min(config.getRetryMaxBackoffMillis(), exponential);
        long backoff =
                jitterBackoff(
                        Math.min(computedBackoff, config.getRetryInitialBackoffMillis()),
                        computedBackoff);
        task.getExecutionContext()
                .getTaskExecutionService()
                .scheduleManagedSourceCoordinatorTimer(
                        () ->
                                requireInternalAdmission(
                                        this::invokeRegistration,
                                        "managed-reader-registration-retry"),
                        backoff);
    }

    private List<SplitT> deserializeSplits(SourceCommandCodec.SplitAssignment assignment)
            throws IOException {
        List<SplitT> splits = new ArrayList<>(assignment.getSplitBytes().size());
        for (int i = 0; i < assignment.getSplitBytes().size(); i++) {
            SplitT split = splitSerializer.deserialize(assignment.getSplitBytes().get(i));
            if (!assignment.getSplitIds().get(i).equals(split.splitId())) {
                throw new IOException("Managed Source split identifier mismatch");
            }
            splits.add(split);
        }
        return splits;
    }

    private void validateCodec(SourceCommandEnvelope command) {
        int expected;
        switch (command.getKind()) {
            case READER_EPOCH_START:
            case PREPARE_CLOSE:
            case CANCEL:
                expected = SourceCommandCodec.EMPTY_CODEC;
                break;
            case NO_MORE_SPLITS:
                expected = SourceCommandCodec.NO_MORE_SPLITS_CODEC;
                break;
            case ASSIGN_SPLITS:
                expected = SourceCommandCodec.SPLIT_ASSIGNMENT_CODEC;
                break;
            case SOURCE_EVENT:
                throw new IllegalArgumentException(
                        "Managed Source protocol version 1 does not support custom Source events");
            case BARRIER:
                expected = SourceCommandCodec.BARRIER_CODEC;
                break;
            case CHECKPOINT_COMPLETE:
            case CHECKPOINT_ABORTED:
            case CHECKPOINT_END:
                expected = SourceCommandCodec.CHECKPOINT_ID_CODEC;
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported managed Source reader command kind " + command.getKind());
        }
        if (command.getPayloadVersion() != SourceCommandCodec.PAYLOAD_VERSION
                || command.getCodecId() != expected) {
            throw new IllegalArgumentException(
                    "Managed Source reader command codec or payload version mismatch");
        }
    }

    private void validateRestoredSelection(ManagedReaderCheckpointState restored) {
        if (restored.getRuntimeMode() != selection.getMode()
                || restored.getRuntimeProtocolVersion() != selection.getRuntimeProtocolVersion()
                || restored.getConnectorStateVersion() != selection.getConnectorStateVersion()
                || !selection.getCapabilityDigest().equals(restored.getCapabilityDigest())) {
            throw new IllegalStateException(
                    "Managed Source reader restore capability or lane mismatch");
        }
    }

    private SourceCommandAdmissionAck ack(
            SourceCommandAdmissionStatus status, SourceCommandEnvelope command, String detail) {
        metrics.recordAdmission(status);
        return SourceCommandAdmissionAck.of(
                status,
                command,
                inboundMailbox.nextSequence(),
                config.getRetryInitialBackoffMillis(),
                detail);
    }

    private boolean admitLocal(CheckedRunnable action, String description, boolean reserved) {
        return admitLocal(action, description, reserved, false, () -> {});
    }

    private boolean admitLocal(
            CheckedRunnable action, String description, boolean reserved, Runnable discardAction) {
        return admitLocal(action, description, reserved, false, discardAction);
    }

    private boolean admitLocal(
            CheckedRunnable action,
            String description,
            boolean reserved,
            boolean runnableWhileBarrierPending,
            Runnable discardAction) {
        synchronized (admissionLock) {
            if (closed || !hasLocalCapacity(reserved)) {
                return false;
            }
            orderedEvents.add(
                    new ControlEvent(
                            action,
                            description,
                            reserved,
                            runnableWhileBarrierPending,
                            discardAction));
            if (!reserved) {
                normalOrderedEvents++;
            }
            return true;
        }
    }

    private boolean admitLocal(CheckedRunnable action, String description) {
        return admitLocal(action, description, false);
    }

    private boolean hasLocalCapacity(boolean reserved) {
        int maxCommands = config.getReaderMailboxMaxCommands();
        int normalMaxCommands = maxCommands - config.getReaderReservedControlCommands();
        return orderedEvents.size() < maxCommands
                && (reserved || normalOrderedEvents < normalMaxCommands);
    }

    private void requireLocalAdmission(CheckedRunnable action, String description) {
        if (!admitLocal(action, description)) {
            asynchronousFailure.compareAndSet(
                    null,
                    new IllegalStateException(
                            "Managed Source reader control mailbox exhausted: " + description));
        }
    }

    private void requireLocalAdmission(
            CheckedRunnable action, String description, boolean reserved) {
        if (!admitLocal(action, description, reserved)) {
            asynchronousFailure.compareAndSet(
                    null,
                    new IllegalStateException(
                            "Managed Source reader reserved control mailbox exhausted: "
                                    + description));
        }
    }

    private void requireInternalAdmission(CheckedRunnable action, String description) {
        if (!admitLocal(action, description, true, true, () -> {})) {
            asynchronousFailure.compareAndSet(
                    null,
                    new IllegalStateException(
                            "Managed Source reader reserved control mailbox exhausted: "
                                    + description));
        }
    }

    private ControlEvent pollNextReadyEvent() {
        if (!lifecycle.isCheckpointBarrierPending()) {
            return orderedEvents.poll();
        }
        Iterator<ControlEvent> iterator = orderedEvents.iterator();
        while (iterator.hasNext()) {
            ControlEvent candidate = iterator.next();
            if (candidate.runnableWhileBarrierPending) {
                iterator.remove();
                return candidate;
            }
        }
        return null;
    }

    private void checkOwner() {
        Thread current = Thread.currentThread();
        if (ownerThread == null) {
            ownerThread = current;
        } else if (ownerThread != current) {
            throw new IllegalStateException(
                    "Managed Source reader state accessed outside its event-loop owner");
        }
    }

    private void updateMetrics() {
        synchronized (admissionLock) {
            metrics.updateReaderState(
                    inboundMailbox,
                    orderedEvents.size(),
                    orderedEvents.size() - normalOrderedEvents,
                    outboundCommands,
                    outboundBytes,
                    appliedCommandWatermark,
                    appliedCommandGaps.size());
        }
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

    private static final class ControlEvent {
        private final CheckedRunnable action;

        @SuppressWarnings("unused")
        private final String description;

        private final boolean reserved;
        private final boolean runnableWhileBarrierPending;
        private final Runnable discardAction;

        private ControlEvent(CheckedRunnable action, String description, boolean reserved) {
            this(action, description, reserved, false, () -> {});
        }

        private ControlEvent(
                CheckedRunnable action,
                String description,
                boolean reserved,
                boolean runnableWhileBarrierPending,
                Runnable discardAction) {
            this.action = action;
            this.description = description;
            this.reserved = reserved;
            this.runnableWhileBarrierPending = runnableWhileBarrierPending;
            this.discardAction = discardAction;
        }

        private void discard() {
            discardAction.run();
        }
    }

    private static final class OutboundCommand {
        private final SourceCommandEnvelope command;
        private final CheckedRunnable acceptedCallback;
        private final long firstAttemptNanos;
        private int attempts;

        private OutboundCommand(
                SourceCommandEnvelope command,
                CheckedRunnable acceptedCallback,
                long firstAttemptNanos) {
            this.command = command;
            this.acceptedCallback = acceptedCallback;
            this.firstAttemptNanos = firstAttemptNanos;
        }
    }

    private static final class BudgetedCollector<T> implements Collector<T> {
        private final SeaTunnelSourceCollector<T> delegate;
        private final ManagedSourceRuntimeMetrics metrics;
        private ManagedPollContext pollContext;

        private BudgetedCollector(
                SeaTunnelSourceCollector<T> delegate, ManagedSourceRuntimeMetrics metrics) {
            this.delegate = delegate;
            this.metrics = metrics;
        }

        private void beginPoll(ManagedPollContext currentPollContext) {
            if (pollContext != null) {
                throw new IllegalStateException("Managed Source collector poll already active");
            }
            pollContext = currentPollContext;
        }

        private void endPoll(ManagedPollContext currentPollContext) {
            if (pollContext != currentPollContext) {
                throw new IllegalStateException("Managed Source collector poll ownership mismatch");
            }
            pollContext = null;
        }

        @Override
        public void collect(T record) {
            ManagedPollContext currentPollContext = requireActivePoll();
            long startedNanos = System.nanoTime();
            int estimatedBytes;
            try {
                estimatedBytes = delegate.collectAndGetSize(record);
            } finally {
                metrics.recordCollectBlocked(Math.max(0L, System.nanoTime() - startedNanos));
            }
            currentPollContext.recordEmitted(estimatedBytes);
        }

        @Override
        public void markSchemaChangeBeforeCheckpoint() {
            requireActivePoll();
            delegate.markSchemaChangeBeforeCheckpoint();
        }

        @Override
        public void collect(SchemaChangeEvent event) {
            requireActivePoll();
            delegate.collect(event);
        }

        @Override
        public void markSchemaChangeAfterCheckpoint() {
            requireActivePoll();
            delegate.markSchemaChangeAfterCheckpoint();
        }

        @Override
        public Object getCheckpointLock() {
            throw new UnsupportedOperationException(
                    "Managed Source connectors must not use the checkpoint lock");
        }

        @Override
        public boolean isEmptyThisPollNext() {
            return delegate.isEmptyThisPollNext();
        }

        @Override
        public void resetEmptyThisPollNext() {
            delegate.resetEmptyThisPollNext();
        }

        private ManagedPollContext requireActivePoll() {
            if (pollContext == null) {
                throw new IllegalStateException(
                        "Managed Source connector emitted outside its poll owner turn");
            }
            return pollContext;
        }
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws Exception;
    }
}
