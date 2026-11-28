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

package org.apache.seatunnel.engine.e2e.pipelineslot;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-wide gates that let the test park a sink writer before it consumes its first row.
 *
 * <p>The sink tasks and the test run in the same JVM because the test starts an in-process Zeta
 * cluster, so a plain latch is enough to freeze one pipeline while the rest of the job keeps
 * running. The latch is closed when it is created: a writer that observes the gate blocks in {@link
 * #await(String)} until the test calls {@link #release(String)}.
 */
public final class PipelineFinishSlotReleaseGate {

    private static final ConcurrentMap<String, CountDownLatch> GATES = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, AtomicLong> WAITING = new ConcurrentHashMap<>();

    private PipelineFinishSlotReleaseGate() {}

    /** Removes every gate so a repeated run starts with closed latches. */
    public static void reset() {
        GATES.clear();
        WAITING.clear();
    }

    /** Number of writers currently parked on {@code key}. */
    public static long waiting(String key) {
        AtomicLong waiting = WAITING.get(key);
        return waiting == null ? 0L : waiting.get();
    }

    /**
     * Blocks the calling writer until the test releases the gate.
     *
     * @param key gate key from the sink options
     */
    public static void await(String key) throws IOException {
        CountDownLatch latch = GATES.computeIfAbsent(key, ignored -> new CountDownLatch(1));
        WAITING.computeIfAbsent(key, ignored -> new AtomicLong()).incrementAndGet();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for gate " + key, e);
        } finally {
            AtomicLong waiting = WAITING.get(key);
            if (waiting != null) {
                waiting.decrementAndGet();
            }
        }
    }

    /** Opens the gate for {@code key}; writers already blocked proceed. */
    public static void release(String key) {
        GATES.computeIfAbsent(key, ignored -> new CountDownLatch(1)).countDown();
    }
}
