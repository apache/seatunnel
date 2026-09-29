/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.translation.flink.schema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.source.SupportSchemaEvolution;

import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import lombok.extern.slf4j.Slf4j;

/**
 * Flink 1.13-specific extension of {@link SchemaOperator} that resolves two issues present when the
 * fallback timer is placed in the common module:
 *
 * <ol>
 *   <li><b>No reflection</b>: {@link ProcessingTimeService} and {@link ProcessingTimeCallback} are
 *       imported directly as strongly-typed Flink 1.13 APIs. There is no risk of silent breakage
 *       from method renames in future Flink versions.
 *   <li><b>No dead flag path</b>: the timer callback fires on the Flink <em>task thread</em> via
 *       {@code ProcessingTimeService.registerTimer}, so {@link #handleFallbackTimerOnTaskThread()}
 *       is always reachable even when no more source data arrives and {@code processElement} is
 *       never called again. This is the exact scenario this workaround targets on Flink 1.13.
 * </ol>
 *
 * <p>The base {@link SchemaOperator} carries none of this timer infrastructure; Flink 1.15 and
 * later use that base class directly because checkpointing behaves correctly there.
 */
@Slf4j
public class SchemaOperator13 extends SchemaOperator {

    /**
     * Guards against double-registration. All accesses happen on the Flink task thread
     * (processElement, timer callbacks, notifyCheckpointComplete)
     */
    private boolean fallbackTimerPending = false;

    public SchemaOperator13(String jobId, SupportSchemaEvolution source, Config pluginConfig) {
        super(jobId, source, pluginConfig);
    }

    /**
     * Registers a processing-time timer that will call {@link #handleFallbackTimerOnTaskThread()}
     * on the Flink task thread after {@link #CHECKPOINT_STALL_TIMEOUT_MS} milliseconds.
     *
     * <p>Using {@link ProcessingTimeService#registerTimer} instead of a background {@code
     * ScheduledExecutorService} achieves two goals:
     *
     * <ul>
     *   <li>The callback is delivered on the task thread, so {@code output.collect} and operator
     *       state are accessed safely without additional synchronisation.
     *   <li>No daemon thread overhead is introduced for Flink 1.14+ users who use the common
     *       module's no-op default.
     * </ul>
     *
     * <p>If a timer is already pending this call is a no-op to prevent duplicate firings.
     */
    @Override
    protected void scheduleFallbackTimer() {
        if (fallbackTimerPending) {
            return;
        }
        fallbackTimerPending = true;

        ProcessingTimeService pts = getProcessingTimeService();
        long fireAt = pts.getCurrentProcessingTime() + CHECKPOINT_STALL_TIMEOUT_MS;

        pts.registerTimer(
                fireAt,
                (ProcessingTimeCallback)
                        timestamp -> {
                            fallbackTimerPending = false;
                            try {
                                handleFallbackTimerOnTaskThread();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                log.error(
                                        "Fallback schema-change timer interrupted for job {}",
                                        jobId,
                                        e);
                            }
                        });

        log.debug(
                "Registered Flink processing-time fallback timer to fire in {}ms for job {}",
                CHECKPOINT_STALL_TIMEOUT_MS,
                jobId);
    }
}
