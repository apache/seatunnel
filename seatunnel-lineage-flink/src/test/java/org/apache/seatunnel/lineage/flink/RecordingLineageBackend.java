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

package org.apache.seatunnel.lineage.flink;

import org.apache.seatunnel.lineage.LineageBackend;
import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageEvent;
import org.apache.seatunnel.lineage.LineageEventType;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Captures the events a hook emits, so a test can assert on them without a receiver.
 *
 * <p>The lists are concurrent because the heartbeat writes from the scheduler thread while the test
 * asserts from its own.
 */
public final class RecordingLineageBackend implements LineageBackend {
    static final String NAME = "recording";

    static final List<LineageEvent> EVENTS = new CopyOnWriteArrayList<>();
    static final List<LineageConfig> CONFIGS = new CopyOnWriteArrayList<>();
    static final BlockingQueue<LineageEvent> EMISSIONS = new LinkedBlockingQueue<>();

    static final AtomicInteger ATTEMPTS = new AtomicInteger();

    /**
     * Holds a heartbeat inside its send, the way an unreachable receiver does, so a test can assert
     * on what happens when a terminal event arrives while a RUNNING is still in flight. Counted
     * down when the send is entered; the send then waits for {@link #HEARTBEAT_RELEASE}.
     */
    static volatile CountDownLatch HEARTBEAT_ENTERED;

    static volatile CountDownLatch HEARTBEAT_RELEASE;

    /**
     * Holds one run inside its send so another job can prove its terminal event is not queued
     * behind it. The blocked run is not recorded until {@link #blockedRelease} opens.
     */
    static volatile UUID blockedRunId;

    static volatile CountDownLatch blockedEntered;

    static volatile CountDownLatch blockedRelease;

    /**
     * Fails the send after {@link #failAfterMs}, the way an unreachable receiver exhausts retries.
     */
    static volatile boolean failDelivery;

    static volatile long failAfterMs;

    static void reset() {
        EVENTS.clear();
        CONFIGS.clear();
        EMISSIONS.clear();
        ATTEMPTS.set(0);
        HEARTBEAT_ENTERED = null;
        HEARTBEAT_RELEASE = null;
        blockedRunId = null;
        blockedEntered = null;
        blockedRelease = null;
        failDelivery = false;
        failAfterMs = 0L;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void emit(LineageConfig config, LineageEvent event) throws IOException {
        ATTEMPTS.incrementAndGet();
        if (failDelivery) {
            if (failAfterMs > 0) {
                try {
                    Thread.sleep(failAfterMs);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("unreachable receiver", interrupted);
                }
            }
            throw new IOException("unreachable receiver");
        }
        UUID blocked = blockedRunId;
        if (blocked != null && blocked.equals(event.runId())) {
            CountDownLatch entered = blockedEntered;
            CountDownLatch release = blockedRelease;
            if (entered != null) {
                entered.countDown();
            }
            if (release != null) {
                try {
                    release.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        CountDownLatch release = HEARTBEAT_RELEASE;
        CountDownLatch entered = HEARTBEAT_ENTERED;
        if (release != null && entered != null && event.eventType() == LineageEventType.RUNNING) {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        CONFIGS.add(config);
        EVENTS.add(event);
        EMISSIONS.offer(event);
    }
}
