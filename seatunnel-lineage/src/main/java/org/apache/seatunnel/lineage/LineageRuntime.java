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

package org.apache.seatunnel.lineage;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The single seam through which every engine emits a lineage event.
 *
 * <p>The event carries its own {@link LineageEventType}, so there is deliberately no per-lifecycle
 * method here: a reporter-style API would need one method per type and would still leave the caller
 * to route the types it does not cover.
 */
public final class LineageRuntime {
    private static final Logger LOGGER = Logger.getLogger(LineageRuntime.class.getName());

    private LineageRuntime() {}

    /**
     * Emits an event through the configured backend, or returns immediately when disabled.
     *
     * <p>Except for {@link VirtualMachineError}, a backend that cannot be resolved (an unknown
     * {@code openlineage_transport}, or a deployment missing the backend's jar/provider file) or
     * one that violates its own no-throw contract only produces a WARNING log line here. Lineage
     * delivery must not be able to change the outcome of the job it is describing, and that
     * guarantee has to hold at this shared entry point rather than depend on every engine call site
     * independently remembering to guard it.
     *
     * @return {@code false} when reporting is disabled or the event was not delivered
     */
    public static boolean emit(LineageConfig config, LineageEvent event) {
        if (config == null || !config.enabled()) {
            return false;
        }
        try {
            return LineageBackendLoader.load(config).tryEmit(config, event);
        } catch (VirtualMachineError e) {
            throw e;
        } catch (Throwable e) {
            LOGGER.log(Level.WARNING, "Failed to emit a lineage event", e);
            return false;
        }
    }

    /**
     * Emits events in order, and stops when one is not delivered or the event-set budget is used.
     *
     * <p>The per-call timeout and retry caps still bound a single send. This budget is what keeps a
     * multi-output job from repeating that full cycle once the receiver is already unreachable. A
     * {@code stop} check that returns {@code true} ends the loop without treating it as a failure,
     * which is how a heartbeat gives way to a terminal event.
     *
     * @param config lineage configuration, ignored when disabled
     * @param events events to send, in the order they must leave
     * @param stop optional check evaluated before each event; {@code null} never stops early
     */
    public static void emitEach(
            LineageConfig config, Iterable<LineageEvent> events, BooleanSupplier stop) {
        if (config == null || !config.enabled() || events == null) {
            return;
        }
        long deadline = System.nanoTime() + eventSetBudgetNanos(config);
        for (LineageEvent event : events) {
            if (stop != null && stop.getAsBoolean()) {
                return;
            }
            if (System.nanoTime() >= deadline) {
                LOGGER.log(
                        Level.WARNING,
                        "Lineage event-set deadline exceeded; skipping the remaining outputs");
                return;
            }
            if (event == null) {
                continue;
            }
            if (!emit(config, event)) {
                LOGGER.log(
                        Level.WARNING, "Lineage delivery failed; skipping the remaining outputs");
                return;
            }
        }
    }

    /**
     * Returns the wall-clock budget for one multi-output event set.
     *
     * <p>One HTTP attempt can spend {@code timeoutMs} in each of the connect, connection-request
     * and socket phases, and a failed delivery repeats that {@code retryTimes + 1} times plus the
     * bounded backoff between attempts. The budget allows one such cycle, with a second of
     * scheduling slack, so later outputs are not started after the first has exhausted its retries.
     * It does not change the per-call timeout or retry caps.
     *
     * @param config resolved lineage configuration
     * @return budget in nanoseconds, always positive
     */
    public static long eventSetBudgetNanos(LineageConfig config) {
        long attempts = (long) config.retryTimes() + 1L;
        long backoffMs = 0L;
        for (int attempt = 0; attempt < config.retryTimes(); attempt++) {
            backoffMs += Math.min(100L, 10L * (attempt + 1L));
        }
        long budgetMs = attempts * 3L * config.timeoutMs() + backoffMs + 1000L;
        return TimeUnit.MILLISECONDS.toNanos(budgetMs);
    }
}
