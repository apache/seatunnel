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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded, sequence-aware Reader mailbox.
 *
 * <p>Admission transfers checkpoint ownership from the sender to the runtime. Commands may arrive
 * out of order, but only the next contiguous sender sequence is exposed to the event loop.
 */
public final class ReaderCommandMailbox {
    private final int maxCommands;
    private final long maxBytes;
    private final int normalMaxCommands;
    private final long normalMaxBytes;
    private final ManagedSourceMemoryBudget workerBudget;
    private final long retryAfterMillis;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition available = lock.newCondition();
    private final TreeMap<Long, SourceCommandEnvelope> commands = new TreeMap<>();
    private final Map<String, Long> commandSequences = new HashMap<>();

    private long nextSequence;
    private long nextAdmissionSequence;
    private int normalCommands;
    private long normalBytes;
    private long totalBytes;
    private boolean closed;

    public ReaderCommandMailbox(
            ManagedSourceRuntimeConfig config,
            ManagedSourceMemoryBudget workerBudget,
            long initialSequence) {
        this.maxCommands = config.getReaderMailboxMaxCommands();
        this.maxBytes = config.getReaderMailboxMaxBytes();
        this.normalMaxCommands = maxCommands - config.getReaderReservedControlCommands();
        this.normalMaxBytes = maxBytes - config.getReaderReservedControlBytes();
        this.workerBudget = workerBudget;
        this.retryAfterMillis = config.getRetryInitialBackoffMillis();
        this.nextSequence = initialSequence;
        this.nextAdmissionSequence = initialSequence;
        if (initialSequence <= 0) {
            throw new IllegalArgumentException("Initial mailbox sequence must be positive");
        }
    }

    /**
     * Admits one command, or explains why it was not admitted.
     *
     * <p>Called from a Hazelcast operation thread, so it only validates, accounts and buffers; it
     * never runs connector code and never waits for the command to be applied.
     *
     * <p>The transport is at-least-once, so admission is the deduplication point. Sequences must
     * arrive contiguously: a gap is answered with {@code RETRY_LATER} rather than buffered, which
     * is what lets {@link #pollNext()} hand commands to the owner in exact sender order. A replay
     * of an already-admitted sequence is {@code DUPLICATE}; the same sequence carrying a different
     * command identifier, or the same identifier at a different sequence, is {@code
     * INVALID_PAYLOAD} because the two sides disagree about history.
     *
     * <p>Capacity is split: control commands may use the reserved band and fail terminally when it
     * is exhausted, while ordinary commands are told to retry so that back-pressure never blocks a
     * barrier or a cancellation.
     *
     * @param command envelope offered by the coordinator
     * @return admission status, with a retry hint when the sender should try again
     */
    public SourceCommandAdmissionAck offer(SourceCommandEnvelope command) {
        int bytes = command.estimatedSizeBytes();
        lock.lock();
        try {
            if (closed) {
                return ack(
                        SourceCommandAdmissionStatus.TERMINAL_REJECTED,
                        command,
                        0L,
                        "Reader mailbox is closed");
            }
            if (command.getSenderSequence() < nextSequence) {
                return ack(SourceCommandAdmissionStatus.DUPLICATE, command, 0L, "Duplicate");
            }
            Long knownSequence = commandSequences.get(command.getCommandId());
            if (knownSequence != null) {
                return ack(
                        knownSequence == command.getSenderSequence()
                                ? SourceCommandAdmissionStatus.DUPLICATE
                                : SourceCommandAdmissionStatus.INVALID_PAYLOAD,
                        command,
                        0L,
                        "Command identifier is already assigned to another sequence");
            }
            SourceCommandEnvelope sequenceOwner = commands.get(command.getSenderSequence());
            if (sequenceOwner != null) {
                SourceCommandAdmissionStatus status =
                        sequenceOwner.getCommandId().equals(command.getCommandId())
                                ? SourceCommandAdmissionStatus.DUPLICATE
                                : SourceCommandAdmissionStatus.INVALID_PAYLOAD;
                return ack(status, command, 0L, "Sender sequence is already occupied");
            }
            if (command.getSenderSequence() != nextAdmissionSequence) {
                return ack(
                        SourceCommandAdmissionStatus.RETRY_LATER,
                        command,
                        retryAfterMillis,
                        "Previous sender sequence has not been admitted");
            }

            boolean reserved = command.usesReservedCapacity();
            if (commands.size() >= maxCommands
                    || bytes > maxBytes - totalBytes
                    || (!reserved
                            && (normalCommands >= normalMaxCommands
                                    || bytes > normalMaxBytes - normalBytes))) {
                return ack(
                        reserved
                                ? SourceCommandAdmissionStatus.TERMINAL_REJECTED
                                : SourceCommandAdmissionStatus.RETRY_LATER,
                        command,
                        retryAfterMillis,
                        reserved
                                ? "Reserved Source control capacity exhausted"
                                : "Reader mailbox capacity exhausted");
            }
            if (!workerBudget.tryReserve(bytes)) {
                return ack(
                        SourceCommandAdmissionStatus.RETRY_LATER,
                        command,
                        retryAfterMillis,
                        "Worker Source mailbox memory budget exhausted");
            }

            SourceCommandEnvelope admitted = command.markAdmitted(System.nanoTime());
            commands.put(admitted.getSenderSequence(), admitted);
            commandSequences.put(admitted.getCommandId(), admitted.getSenderSequence());
            nextAdmissionSequence = Math.addExact(nextAdmissionSequence, 1L);
            totalBytes += bytes;
            if (!reserved) {
                normalCommands++;
                normalBytes += bytes;
            }
            available.signalAll();
            return ack(SourceCommandAdmissionStatus.ACCEPTED, admitted, 0L, "");
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes the next command in sender order, or returns {@code null} if it has not arrived.
     *
     * <p>Only the next contiguous sequence is ever returned, so the owner applies commands in the
     * order the coordinator sent them regardless of the order they were received in. Releases the
     * command's share of the mailbox and worker memory budgets.
     *
     * <p>Called by the Reader event-loop owner.
     *
     * @return the next command in sequence, or {@code null} when the mailbox has a gap or is empty
     */
    public SourceCommandEnvelope pollNext() {
        lock.lock();
        try {
            SourceCommandEnvelope command = commands.remove(nextSequence);
            if (command == null) {
                return null;
            }
            commandSequences.remove(command.getCommandId());
            int bytes = command.estimatedSizeBytes();
            totalBytes -= bytes;
            if (!command.usesReservedCapacity()) {
                normalCommands--;
                normalBytes -= bytes;
            }
            workerBudget.release(bytes);
            nextSequence = Math.addExact(nextSequence, 1L);
            return command;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Rebases the expected sender sequence when a new coordinator epoch takes over.
     *
     * <p>A new coordinator restarts its channel numbering, so without rebasing every command from
     * it would look like a stale duplicate. Refuses to rebase while commands are still admitted,
     * because those belong to the previous epoch and applying them afterwards would interleave two
     * coordinators' histories.
     *
     * @param firstSequence first sequence the new epoch will send, must be positive
     */
    public void resetForEpoch(long firstSequence) {
        lock.lock();
        try {
            if (!commands.isEmpty()) {
                throw new IllegalStateException(
                        "Cannot change Reader command epoch with admitted commands");
            }
            if (firstSequence <= 0) {
                throw new IllegalArgumentException("Epoch sequence must be positive");
            }
            nextSequence = firstSequence;
            nextAdmissionSequence = firstSequence;
            available.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits until the next in-order command is available, the mailbox closes, or the timeout
     * elapses.
     *
     * <p>Bounded on purpose: the owner must come back to poll its local control queue and check
     * terminal signals even when no remote command ever arrives. Returns immediately when the next
     * sequence is already present or the mailbox is closed.
     *
     * @param timeoutMillis maximum time to wait
     * @throws InterruptedException if the owner thread is interrupted while waiting
     */
    public void awaitSignal(long timeoutMillis) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            if (!closed && !commands.containsKey(nextSequence)) {
                available.await(timeoutMillis, TimeUnit.MILLISECONDS);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Wakes an owner blocked in {@link #awaitSignal(long)} without admitting a command.
     *
     * <p>Used by asynchronous failure and terminal paths so the owner observes a signal it would
     * otherwise only notice after its wait timed out.
     */
    public void signal() {
        lock.lock();
        try {
            available.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes the mailbox, discards buffered commands and returns their memory to the worker budget.
     *
     * <p>Releasing the worker budget here is what stops a cancelled or failed Source task from
     * permanently shrinking the memory available to the other Sources on the same worker. After
     * close, {@link #offer(SourceCommandEnvelope)} rejects terminally and any waiting owner is
     * woken.
     */
    public void close() {
        lock.lock();
        try {
            closed = true;
            for (SourceCommandEnvelope command : commands.values()) {
                workerBudget.release(command.estimatedSizeBytes());
            }
            commands.clear();
            commandSequences.clear();
            normalCommands = 0;
            normalBytes = 0;
            totalBytes = 0;
            available.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return commands.size();
        } finally {
            lock.unlock();
        }
    }

    public long bytes() {
        lock.lock();
        try {
            return totalBytes;
        } finally {
            lock.unlock();
        }
    }

    /** Returns the current number of commands using reserved control capacity. */
    public int reservedCommands() {
        lock.lock();
        try {
            return commands.size() - normalCommands;
        } finally {
            lock.unlock();
        }
    }

    /** Returns the current payload and envelope bytes using reserved control capacity. */
    public long reservedBytes() {
        lock.lock();
        try {
            return totalBytes - normalBytes;
        } finally {
            lock.unlock();
        }
    }

    public long nextSequence() {
        lock.lock();
        try {
            return nextSequence;
        } finally {
            lock.unlock();
        }
    }

    public long oldestCommandAgeNanos(long nowNanos) {
        lock.lock();
        try {
            long oldest = Long.MAX_VALUE;
            for (SourceCommandEnvelope command : commands.values()) {
                if (command.getAdmittedNanos() > 0) {
                    oldest = Math.min(oldest, command.getAdmittedNanos());
                }
            }
            return oldest == Long.MAX_VALUE ? 0L : Math.max(0L, nowNanos - oldest);
        } finally {
            lock.unlock();
        }
    }

    private SourceCommandAdmissionAck ack(
            SourceCommandAdmissionStatus status,
            SourceCommandEnvelope command,
            long retry,
            String detail) {
        return SourceCommandAdmissionAck.of(status, command, nextAdmissionSequence, retry, detail);
    }
}
