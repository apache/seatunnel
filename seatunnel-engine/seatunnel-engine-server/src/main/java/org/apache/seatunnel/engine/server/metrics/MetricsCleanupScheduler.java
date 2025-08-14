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

package org.apache.seatunnel.engine.server.metrics;

import org.apache.seatunnel.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MetricsCleanupScheduler {
    private final int cleanupRetryIntervalSeconds;
    private long lastCleanupTime = 0L;
    private final ScheduledExecutorService cleanupScheduler;
    private final BlockingQueue<PipelineLocation> metricsCleanupRetryQueue;

    public MetricsCleanupScheduler(
            int cleanupRetryIntervalSeconds,
            BlockingQueue<PipelineLocation> metricsCleanupRetryQueue) {
        if (cleanupRetryIntervalSeconds <= 0) {
            CommonError.illegalArgument(
                    "cleanupRetryInterval", "Cleanup retry interval must be positive");
        }
        this.cleanupRetryIntervalSeconds = cleanupRetryIntervalSeconds;
        this.metricsCleanupRetryQueue = metricsCleanupRetryQueue;
        this.cleanupScheduler =
                Executors.newScheduledThreadPool(
                        1,
                        new ThreadFactoryBuilder()
                                .setNameFormat("metrics-cleanup-scheduler-%d")
                                .build());
    }

    public void start(Runnable cleaner) {
        cleanupScheduler.scheduleWithFixedDelay(
                () -> scheduledCleanupWithDelayCheck(cleaner),
                0,
                cleanupRetryIntervalSeconds,
                TimeUnit.SECONDS);
        log.info(
                "Metrics cleanup scheduler started with interval: {} seconds",
                cleanupRetryIntervalSeconds);
    }

    public boolean offerRetryQueue(PipelineLocation pipelineLocation) {
        if (metricsCleanupRetryQueue.remainingCapacity() == 0) {
            PipelineLocation removedData = metricsCleanupRetryQueue.poll();
            log.info("Removed old pipelineLocation from retry queue: {}", removedData);
        }
        boolean offer = metricsCleanupRetryQueue.offer(pipelineLocation);
        if (!offer) {
            log.warn("Failed to add pipelineLocation to retry queue: {}", pipelineLocation);
        } else {
            updateLastCleanupTime();
        }
        return offer;
    }

    private void updateLastCleanupTime() {
        this.lastCleanupTime = System.currentTimeMillis();
    }

    private void scheduledCleanupWithDelayCheck(Runnable cleaner) {
        long now = System.currentTimeMillis();
        long elapsed = now - lastCleanupTime;
        long waitMillis = cleanupRetryIntervalSeconds * 1000L - elapsed;

        if (waitMillis > 0) {
            try {
                Thread.sleep(waitMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        cleaner.run();
    }

    public void stop() {
        cleanupScheduler.shutdown();
        try {
            if (!cleanupScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn(
                        "Metrics cleanup scheduler did not terminate in 10 seconds, forcing shutdown...");
                cleanupScheduler.shutdownNow();
                if (!cleanupScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.warn(
                            "Metrics cleanup scheduler failed to terminate even after shutdownNow()");
                }
            }
        } catch (InterruptedException e) {
            throw new SeaTunnelEngineException("wait clean scheduled executor service error", e);
        }
    }
}
