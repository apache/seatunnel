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

package org.apache.seatunnel.engine.server.resourcemanager.autoscaler;

import org.apache.seatunnel.engine.common.config.server.AutoscalerConfig;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import com.hazelcast.cluster.Address;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Worker autoscaler that monitors cluster metrics and makes scaling recommendations.
 *
 * <p>This autoscaler runs on the master node and periodically evaluates cluster load, slot usage,
 * and worker utilization to determine whether scale-out or scale-in actions are recommended.
 *
 * <p>Phase 1: recommendation-only mode. The autoscaler emits scaling recommendations via REST API
 * and metrics but does not directly provision or decommission workers. External systems (Kubernetes
 * HPA, custom operators) can consume these recommendations.
 */
@Slf4j
public class WorkerAutoscaler {

    private final AutoscalerConfig config;
    private final ResourceManager resourceManager;
    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;

    // Scaling state
    private final AtomicReference<Instant> lastScaleOutTime = new AtomicReference<>();
    private final AtomicReference<Instant> lastScaleInTime = new AtomicReference<>();

    // Metric history for stabilization windows
    private final ConcurrentMap<MetricType, SlidingWindow> metricWindows =
            new ConcurrentHashMap<>();

    // Current recommendation
    @Getter
    private volatile AutoscalerRecommendation currentRecommendation =
            AutoscalerRecommendation.noAction();

    @Getter private volatile AutoscalerState autoscalerState = new AutoscalerState();

    public WorkerAutoscaler(AutoscalerConfig config, ResourceManager resourceManager) {
        this.config = config;
        this.resourceManager = resourceManager;
        for (MetricType type : MetricType.values()) {
            metricWindows.put(
                    type,
                    new SlidingWindow(
                            config.getStabilizationWindowSeconds(),
                            config.getEvaluationIntervalSeconds()));
        }
    }

    /** Start the autoscaler control loop. */
    public synchronized void start() {
        if (running) {
            return;
        }
        if (!config.isEnabled()) {
            log.info("Worker autoscaler is disabled, not starting.");
            return;
        }
        log.info(
                "Starting WorkerAutoscaler: minWorkers={}, maxWorkers={}, recommendationOnly={}, "
                        + "evaluationInterval={}s, stabilizationWindow={}s, "
                        + "scaleOutCooldown={}s, scaleInCooldown={}s",
                config.getMinWorkers(),
                config.getMaxWorkers(),
                config.isRecommendationOnly(),
                config.getEvaluationIntervalSeconds(),
                config.getStabilizationWindowSeconds(),
                config.getScaleOutCooldownSeconds(),
                config.getScaleInCooldownSeconds());

        running = true;
        scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "worker-autoscaler");
                            t.setDaemon(true);
                            return t;
                        });
        scheduler.scheduleAtFixedRate(
                this::evaluate,
                config.getEvaluationIntervalSeconds(),
                config.getEvaluationIntervalSeconds(),
                TimeUnit.SECONDS);
    }

    /** Stop the autoscaler. */
    public synchronized void shutdown() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        log.info("WorkerAutoscaler stopped.");
    }

    /** Main evaluation loop. */
    void evaluate() {
        if (!running) {
            return;
        }
        try {
            ConcurrentMap<Address, WorkerProfile> workers = resourceManager.getRegisterWorker();
            if (workers.isEmpty()) {
                log.debug("No registered workers, skipping autoscaler evaluation.");
                return;
            }

            int currentWorkerCount = workers.size();
            int totalSlots = computeTotalSlots(workers);
            int assignedSlots = computeAssignedSlots(workers);
            double slotUsageRatio =
                    totalSlots > 0 ? (double) assignedSlots / (double) totalSlots : 0.0;
            double avgCpu = computeAverageCpuLoad(workers);
            double avgMemory = computeAverageMemoryLoad(workers);

            // Record metrics
            recordMetric(MetricType.SLOT_USAGE, slotUsageRatio);
            recordMetric(MetricType.CPU_LOAD, avgCpu);
            recordMetric(MetricType.MEMORY_LOAD, avgMemory);
            recordMetric(MetricType.WORKER_COUNT, currentWorkerCount);

            // Update state
            autoscalerState =
                    new AutoscalerState(
                            currentWorkerCount,
                            totalSlots,
                            assignedSlots,
                            slotUsageRatio,
                            avgCpu,
                            avgMemory,
                            config.getMinWorkers(),
                            config.getMaxWorkers());

            // Evaluate scaling decision
            ScalingDecision decision = evaluateScalingDecision(currentWorkerCount, workers);

            // Apply cooldown
            decision = applyCooldown(decision);

            // Create recommendation
            AutoscalerRecommendation recommendation =
                    buildRecommendation(
                            decision,
                            currentWorkerCount,
                            workers,
                            slotUsageRatio,
                            avgCpu,
                            avgMemory);
            currentRecommendation = recommendation;

            if (recommendation.getAction() != ScalingAction.NO_ACTION) {
                log.info(
                        "Autoscaler recommendation: action={}, reason={}, targetWorkers={}, "
                                + "currentWorkers={}, slotUsage={}, avgCpu={}, avgMem={}",
                        recommendation.getAction(),
                        recommendation.getReason(),
                        recommendation.getTargetWorkerCount(),
                        currentWorkerCount,
                        String.format("%.2f", slotUsageRatio),
                        String.format("%.2f", avgCpu),
                        String.format("%.2f", avgMemory));
            }
        } catch (Exception e) {
            log.warn("Error during autoscaler evaluation: {}", e.getMessage(), e);
        }
    }

    private ScalingDecision evaluateScalingDecision(
            int currentWorkerCount, ConcurrentMap<Address, WorkerProfile> workers) {

        double avgCpu = getStableMetric(MetricType.CPU_LOAD);
        double avgMemory = getStableMetric(MetricType.MEMORY_LOAD);
        double avgSlotUsage = getStableMetric(MetricType.SLOT_USAGE);

        // Check scale-out conditions
        boolean cpuScaleOut = avgCpu > config.getCpuScaleOutThreshold();
        boolean memoryScaleOut = avgMemory > config.getMemoryScaleOutThreshold();
        boolean slotScaleOut = avgSlotUsage > config.getSlotUsageScaleOutThreshold();

        if ((cpuScaleOut || memoryScaleOut || slotScaleOut)
                && currentWorkerCount < config.getMaxWorkers()) {
            int targetWorkers = Math.min(currentWorkerCount + 1, config.getMaxWorkers());
            String reason =
                    buildScaleOutReason(
                            cpuScaleOut,
                            memoryScaleOut,
                            slotScaleOut,
                            avgCpu,
                            avgMemory,
                            avgSlotUsage);
            return new ScalingDecision(ScalingAction.SCALE_OUT, targetWorkers, reason);
        }

        // Check scale-in conditions
        boolean cpuScaleIn = avgCpu < config.getCpuScaleInThreshold();
        boolean memoryScaleIn = avgMemory < config.getMemoryScaleInThreshold();
        boolean slotScaleIn = avgSlotUsage < config.getSlotUsageScaleInThreshold();

        if (cpuScaleIn
                && memoryScaleIn
                && slotScaleIn
                && currentWorkerCount > config.getMinWorkers()) {
            int targetWorkers = Math.max(currentWorkerCount - 1, config.getMinWorkers());
            String reason = buildScaleInReason(avgCpu, avgMemory, avgSlotUsage);
            return new ScalingDecision(ScalingAction.SCALE_IN, targetWorkers, reason);
        }

        return ScalingDecision.noAction();
    }

    private String buildScaleOutReason(
            boolean cpu,
            boolean memory,
            boolean slot,
            double avgCpu,
            double avgMem,
            double avgSlot) {
        StringBuilder sb = new StringBuilder("Scale-out triggered by: ");
        if (cpu) {
            sb.append(
                    String.format(
                            "CPU=%.2f (threshold=%.2f) ",
                            avgCpu, config.getCpuScaleOutThreshold()));
        }
        if (memory) {
            sb.append(
                    String.format(
                            "Memory=%.2f (threshold=%.2f) ",
                            avgMem, config.getMemoryScaleOutThreshold()));
        }
        if (slot) {
            sb.append(
                    String.format(
                            "SlotUsage=%.2f (threshold=%.2f) ",
                            avgSlot, config.getSlotUsageScaleOutThreshold()));
        }
        return sb.toString().trim();
    }

    private String buildScaleInReason(double avgCpu, double avgMem, double avgSlot) {
        return String.format(
                "Scale-in: all metrics below thresholds. CPU=%.2f (threshold=%.2f), "
                        + "Memory=%.2f (threshold=%.2f), SlotUsage=%.2f (threshold=%.2f)",
                avgCpu,
                config.getCpuScaleInThreshold(),
                avgMem,
                config.getMemoryScaleInThreshold(),
                avgSlot,
                config.getSlotUsageScaleInThreshold());
    }

    private ScalingDecision applyCooldown(ScalingDecision decision) {
        if (decision.getAction() == ScalingAction.SCALE_OUT) {
            Instant last = lastScaleOutTime.get();
            if (last != null) {
                long elapsed = Instant.now().getEpochSecond() - last.getEpochSecond();
                if (elapsed < config.getScaleOutCooldownSeconds()) {
                    log.debug(
                            "Scale-out cooldown active: {}s remaining",
                            config.getScaleOutCooldownSeconds() - elapsed);
                    return ScalingDecision.noAction();
                }
            }
            lastScaleOutTime.set(Instant.now());
        } else if (decision.getAction() == ScalingAction.SCALE_IN) {
            Instant last = lastScaleInTime.get();
            if (last != null) {
                long elapsed = Instant.now().getEpochSecond() - last.getEpochSecond();
                if (elapsed < config.getScaleInCooldownSeconds()) {
                    log.debug(
                            "Scale-in cooldown active: {}s remaining",
                            config.getScaleInCooldownSeconds() - elapsed);
                    return ScalingDecision.noAction();
                }
            }
            lastScaleInTime.set(Instant.now());
        }
        return decision;
    }

    private AutoscalerRecommendation buildRecommendation(
            ScalingDecision decision,
            int currentWorkers,
            ConcurrentMap<Address, WorkerProfile> workers,
            double slotUsage,
            double avgCpu,
            double avgMem) {
        if (decision.getAction() == ScalingAction.NO_ACTION) {
            return AutoscalerRecommendation.noAction();
        }

        return AutoscalerRecommendation.builder()
                .action(decision.getAction())
                .reason(decision.getReason())
                .currentWorkerCount(currentWorkers)
                .targetWorkerCount(decision.getTargetWorkers())
                .recommendationOnly(config.isRecommendationOnly())
                .slotUsageRatio(slotUsage)
                .averageCpuLoad(avgCpu)
                .averageMemoryLoad(avgMem)
                .timestamp(Instant.now())
                .build();
    }

    // --- Metric helpers ---

    private void recordMetric(MetricType type, double value) {
        SlidingWindow window = metricWindows.get(type);
        if (window != null) {
            window.add(value);
        }
    }

    private double getStableMetric(MetricType type) {
        SlidingWindow window = metricWindows.get(type);
        return window != null ? window.getAverage() : 0.0;
    }

    private int computeTotalSlots(ConcurrentMap<Address, WorkerProfile> workers) {
        return workers.values().stream()
                .mapToInt(
                        w -> {
                            SlotProfile[] assigned = w.getAssignedSlots();
                            SlotProfile[] unassigned = w.getUnassignedSlots();
                            int assignedCount = assigned != null ? assigned.length : 0;
                            int unassignedCount = unassigned != null ? unassigned.length : 0;
                            return assignedCount + unassignedCount;
                        })
                .sum();
    }

    private int computeAssignedSlots(ConcurrentMap<Address, WorkerProfile> workers) {
        return workers.values().stream()
                .mapToInt(
                        w -> {
                            SlotProfile[] assigned = w.getAssignedSlots();
                            return assigned != null ? assigned.length : 0;
                        })
                .sum();
    }

    private double computeAverageCpuLoad(ConcurrentMap<Address, WorkerProfile> workers) {
        OptionalDouble avg =
                workers.values().stream()
                        .filter(
                                w ->
                                        w.getSystemLoadInfo() != null
                                                && !Double.isNaN(
                                                        w.getSystemLoadInfo().getCpuPercentage()))
                        .mapToDouble(w -> w.getSystemLoadInfo().getCpuPercentage())
                        .average();
        return avg.orElse(0.0);
    }

    private double computeAverageMemoryLoad(ConcurrentMap<Address, WorkerProfile> workers) {
        OptionalDouble avg =
                workers.values().stream()
                        .filter(
                                w ->
                                        w.getSystemLoadInfo() != null
                                                && !Double.isNaN(
                                                        w.getSystemLoadInfo().getMemPercentage()))
                        .mapToDouble(w -> w.getSystemLoadInfo().getMemPercentage())
                        .average();
        return avg.orElse(0.0);
    }

    // --- Inner types ---

    enum MetricType {
        CPU_LOAD,
        MEMORY_LOAD,
        SLOT_USAGE,
        WORKER_COUNT
    }

    /** Sliding window that maintains average value over a time window. */
    static class SlidingWindow {
        private final int maxSize;
        private final double[] values;
        private int index;
        private int count;

        SlidingWindow(int windowSeconds, int intervalSeconds) {
            this.maxSize = Math.max(1, windowSeconds / Math.max(1, intervalSeconds));
            this.values = new double[maxSize];
            this.index = 0;
            this.count = 0;
        }

        synchronized void add(double value) {
            values[index] = value;
            index = (index + 1) % maxSize;
            if (count < maxSize) {
                count++;
            }
        }

        synchronized double getAverage() {
            if (count == 0) {
                return 0.0;
            }
            double sum = 0;
            for (int i = 0; i < count; i++) {
                sum += values[i];
            }
            return sum / count;
        }
    }

    @Getter
    @ToString
    public static class ScalingDecision {
        private final ScalingAction action;
        private final int targetWorkers;
        private final String reason;

        ScalingDecision(ScalingAction action, int targetWorkers, String reason) {
            this.action = action;
            this.targetWorkers = targetWorkers;
            this.reason = reason;
        }

        static ScalingDecision noAction() {
            return new ScalingDecision(ScalingAction.NO_ACTION, -1, "No scaling needed");
        }
    }

    @Getter
    @ToString
    public static class AutoscalerState {
        private final int currentWorkerCount;
        private final int totalSlots;
        private final int assignedSlots;
        private final double slotUsageRatio;
        private final double averageCpuLoad;
        private final double averageMemoryLoad;
        private final int minWorkers;
        private final int maxWorkers;

        public AutoscalerState() {
            this(0, 0, 0, 0.0, 0.0, 0.0, 0, 0);
        }

        public AutoscalerState(
                int currentWorkerCount,
                int totalSlots,
                int assignedSlots,
                double slotUsageRatio,
                double averageCpuLoad,
                double averageMemoryLoad,
                int minWorkers,
                int maxWorkers) {
            this.currentWorkerCount = currentWorkerCount;
            this.totalSlots = totalSlots;
            this.assignedSlots = assignedSlots;
            this.slotUsageRatio = slotUsageRatio;
            this.averageCpuLoad = averageCpuLoad;
            this.averageMemoryLoad = averageMemoryLoad;
            this.minWorkers = minWorkers;
            this.maxWorkers = maxWorkers;
        }
    }
}
