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

import java.util.concurrent.atomic.AtomicLong;

/** Worker-wide bounded memory reservation shared by all managed Source mailboxes. */
public final class ManagedSourceMemoryBudget {
    private final long maxBytes;
    private final AtomicLong usedBytes = new AtomicLong();

    public ManagedSourceMemoryBudget(long maxBytes) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("Managed Source memory budget must be positive");
        }
        this.maxBytes = maxBytes;
    }

    public boolean tryReserve(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("Reservation bytes must not be negative");
        }
        while (true) {
            long current = usedBytes.get();
            if (bytes > maxBytes - current) {
                return false;
            }
            if (usedBytes.compareAndSet(current, current + bytes)) {
                return true;
            }
        }
    }

    public void release(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("Released bytes must not be negative");
        }
        while (true) {
            long current = usedBytes.get();
            if (bytes > current) {
                throw new IllegalStateException("Managed Source memory budget released below zero");
            }
            if (usedBytes.compareAndSet(current, current - bytes)) {
                return;
            }
        }
    }

    public long getUsedBytes() {
        return usedBytes.get();
    }

    public long getMaxBytes() {
        return maxBytes;
    }
}
