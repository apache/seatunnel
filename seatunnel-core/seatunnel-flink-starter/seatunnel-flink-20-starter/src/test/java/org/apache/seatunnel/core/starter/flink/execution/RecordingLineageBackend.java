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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.lineage.LineageBackend;
import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageEvent;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Captures the events the registration path emits, so a test can assert which lifecycle events a
 * submission produced rather than only that nothing was thrown.
 *
 * <p>Selected through the {@code LineageBackend} SPI by naming {@link #NAME} as the transport.
 */
public final class RecordingLineageBackend implements LineageBackend {
    static final String NAME = "recording";

    static final List<LineageEvent> EVENTS = new CopyOnWriteArrayList<>();

    static final AtomicInteger ATTEMPTS = new AtomicInteger();

    /** Fails the send after {@link #failAfterMs}, as an unreachable receiver would. */
    static volatile boolean failDelivery;

    static volatile long failAfterMs;

    static void reset() {
        EVENTS.clear();
        ATTEMPTS.set(0);
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
        EVENTS.add(event);
    }
}
