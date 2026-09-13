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

import org.apache.seatunnel.api.source.managed.PollContext;

/** Mutable poll budget owned by one Reader event-loop turn. */
public final class ManagedPollContext implements PollContext {
    private final long deadlineNanos;
    private int remainingRecords;
    private long remainingBytes;
    private int emittedRecords;
    private long emittedBytes;

    public ManagedPollContext(int maxRecords, long maxBytes, long deadlineNanos) {
        if (maxRecords <= 0 || maxBytes <= 0) {
            throw new IllegalArgumentException("Managed poll budgets must be positive");
        }
        this.remainingRecords = maxRecords;
        this.remainingBytes = maxBytes;
        this.deadlineNanos = deadlineNanos;
    }

    @Override
    public int remainingRecords() {
        return remainingRecords;
    }

    @Override
    public long remainingBytes() {
        return remainingBytes;
    }

    @Override
    public long deadlineNanos() {
        return deadlineNanos;
    }

    @Override
    public boolean shouldYield() {
        return remainingRecords <= 0 || remainingBytes <= 0 || System.nanoTime() >= deadlineNanos;
    }

    @Override
    public void recordEmitted(long estimatedBytes) {
        long boundedBytes = Math.max(0L, estimatedBytes);
        emittedRecords++;
        emittedBytes = Math.addExact(emittedBytes, boundedBytes);
        remainingRecords = Math.max(0, remainingRecords - 1);
        remainingBytes = Math.max(0L, remainingBytes - boundedBytes);
    }

    /** Returns the number of records emitted by this turn. */
    public int emittedRecords() {
        return emittedRecords;
    }

    /** Returns the estimated payload bytes emitted by this turn. */
    public long emittedBytes() {
        return emittedBytes;
    }
}
