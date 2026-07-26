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

    public void signal() {
        lock.lock();
        try {
            available.signalAll();
        } finally {
            lock.unlock();
        }
    }

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
