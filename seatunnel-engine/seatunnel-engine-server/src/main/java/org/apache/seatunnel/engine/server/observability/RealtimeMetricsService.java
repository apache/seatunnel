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

package org.apache.seatunnel.engine.server.observability;

import org.apache.seatunnel.api.common.metrics.RawJobMetrics;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import com.hazelcast.internal.metrics.MetricConsumer;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.impl.MetricsCompressor;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.metrics.MetricNames.INTERMEDIATE_QUEUE_CAPACITY;
import static org.apache.seatunnel.api.common.metrics.MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.INTERMEDIATE_QUEUE_SIZE;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_ABORT_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_COMMIT_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_PREPARE_COMMIT_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_RECORDS_IN;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_IDLE_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_READ_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.TRANSFORM_PROCESS_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.TRANSFORM_RECORDS_IN;
import static org.apache.seatunnel.api.common.metrics.MetricNames.TRANSFORM_RECORDS_OUT;

/**
 * Active-master service that collects and serves recent-window realtime metrics for enabled jobs.
 *
 * <p>The service is started and stopped by the master-election lifecycle. A single background
 * collector periodically fetches targeted worker metric prefixes, aggregates them into bounded
 * in-memory buckets, and exposes best-effort snapshots to the REST layer. Shutdown stops the
 * collector and clears all cached job state so a standby or stopped node does not serve stale data.
 */
@Slf4j
public class RealtimeMetricsService {

    private static final long POLL_INTERVAL_MS = 5000L;
    private static final long METRICS_FETCH_TIMEOUT_MS = 3000L;
    private static final String JOB_ID_TAG =
            org.apache.seatunnel.api.common.metrics.MetricTags.JOB_ID;
    private static final String[] METRIC_PREFIX_FILTER =
            new String[] {
                INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS,
                INTERMEDIATE_QUEUE_SIZE,
                INTERMEDIATE_QUEUE_CAPACITY,
                SOURCE_READ_NANOS,
                SOURCE_IDLE_NANOS,
                TRANSFORM_PROCESS_NANOS,
                TRANSFORM_RECORDS_IN,
                TRANSFORM_RECORDS_OUT,
                SINK_WRITE_NANOS,
                SINK_RECORDS_IN,
                SINK_PREPARE_COMMIT_NANOS,
                SINK_COMMIT_NANOS,
                SINK_ABORT_NANOS
            };

    private final NodeEngineImpl nodeEngine;
    private final CoordinatorService coordinatorService;
    private final ScheduledExecutorService scheduler;

    private final ConcurrentMap<Long, JobStore> jobStores = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, JobMeta> jobMetas = new ConcurrentHashMap<>();

    private volatile long lastCollectStartMs = -1;
    private volatile long lastCollectEndMs = -1;
    private volatile long lastRawMetricsFetchCostMs = -1;
    private volatile int lastRawMetricsBlobs = 0;
    private volatile String lastCollectError = null;
    private final AtomicLong collectFailureCount = new AtomicLong(0);

    public RealtimeMetricsService(
            NodeEngineImpl nodeEngine, CoordinatorService coordinatorService) {
        this.nodeEngine = nodeEngine;
        this.coordinatorService = coordinatorService;
        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactory() {
                            @Override
                            public Thread newThread(Runnable r) {
                                Thread t = new Thread(r, "realtime-metrics-collector");
                                t.setDaemon(true);
                                return t;
                            }
                        });
    }

    /** Starts periodic collection on the active master. */
    public void start() {
        scheduler.scheduleWithFixedDelay(
                this::collectSafely, 0, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /** Stops collection and releases all in-memory metric windows. */
    public void shutdown() {
        scheduler.shutdownNow();
        jobStores.clear();
        jobMetas.clear();
    }

    /** Returns a lightweight summary of running jobs and their observability status. */
    public List<Map<String, Object>> listJobs() {
        return jobMetas.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(
                        e -> {
                            JobStore store = jobStores.get(e.getKey());
                            Map<String, Object> m = new HashMap<>();
                            m.put("jobId", e.getKey());
                            m.put("enabled", e.getValue().enabled);
                            m.put("bucketMs", e.getValue().bucketMs);
                            m.put("retentionMinutes", e.getValue().retentionMinutes);
                            m.put(
                                    "latestBucketStartMs",
                                    store == null ? -1 : store.latestBucketStartMs);
                            return m;
                        })
                .collect(Collectors.toList());
    }

    /** Returns recent queue/backpressure metrics for the requested job edge window. */
    public Map<String, Object> getJobEdges(long jobId, long windowMs) {
        JobStore store = jobStores.get(jobId);
        if (store != null) {
            return store.toEdgesResponse(windowMs);
        }
        JobMeta meta = jobMetas.get(jobId);
        if (meta == null) {
            return Collections.emptyMap();
        }
        return emptyEdgesResponse(meta.bucketMs, windowMs, meta.enabled);
    }

    /** Returns recent source, transform, and sink busy metrics for the requested job window. */
    public Map<String, Object> getJobVertices(long jobId, long windowMs) {
        JobStore store = jobStores.get(jobId);
        if (store != null) {
            return store.toVerticesResponse(windowMs);
        }
        JobMeta meta = jobMetas.get(jobId);
        if (meta == null) {
            return Collections.emptyMap();
        }
        return emptyVerticesResponse(meta.bucketMs, windowMs, meta.enabled);
    }

    /** Returns collector lifecycle and last-collection diagnostics for REST responses. */
    public Map<String, Object> getCollectorStatus() {
        Map<String, Object> m = new HashMap<>();
        m.put("pollIntervalMs", POLL_INTERVAL_MS);
        m.put("fetchTimeoutMs", METRICS_FETCH_TIMEOUT_MS);
        m.put("lastCollectStartMs", lastCollectStartMs);
        m.put("lastCollectEndMs", lastCollectEndMs);
        m.put("lastRawMetricsFetchCostMs", lastRawMetricsFetchCostMs);
        m.put("lastRawMetricsBlobs", lastRawMetricsBlobs);
        m.put("collectFailureCount", collectFailureCount.get());
        if (lastCollectError != null) {
            m.put("lastCollectError", lastCollectError);
        }
        return m;
    }

    private void collectSafely() {
        lastCollectStartMs = System.currentTimeMillis();
        try {
            collectOnce();
            lastCollectError = null;
        } catch (Throwable t) {
            collectFailureCount.incrementAndGet();
            lastCollectError = t.getClass().getSimpleName() + ": " + t.getMessage();
            log.warn("Collect realtime metrics failed", t);
        } finally {
            lastCollectEndMs = System.currentTimeMillis();
        }
    }

    private void collectOnce() {
        Set<Long> runningJobIds = getRunningJobIds();

        jobStores.keySet().removeIf(jobId -> !runningJobIds.contains(jobId));
        jobMetas.keySet().removeIf(jobId -> !runningJobIds.contains(jobId));

        long nowMs = System.currentTimeMillis();
        Set<Long> enabledJobIds = new HashSet<>();
        for (Long jobId : runningJobIds) {
            ObservabilityConfig config = resolveObservabilityConfig(jobId);
            jobMetas.put(
                    jobId,
                    new JobMeta(
                            config.isEnabled(),
                            config.getBucketMs(),
                            config.getRetentionMinutes(),
                            config.getJobName(),
                            config.getCreateTime()));
            if (!config.isEnabled()) {
                jobStores.remove(jobId);
                continue;
            }
            enabledJobIds.add(jobId);
            jobStores.compute(
                    jobId,
                    (k, store) -> {
                        if (store == null
                                || store.bucketMs != config.getBucketMs()
                                || store.retentionMinutes != config.getRetentionMinutes()) {
                            return new JobStore(config.getBucketMs(), config.getRetentionMinutes());
                        }
                        return store;
                    });
        }

        if (enabledJobIds.isEmpty()) {
            return;
        }

        long fetchStartMs = System.currentTimeMillis();
        List<RawJobMetrics> rawMetrics =
                coordinatorService.getRunningJobRawMetrics(
                        enabledJobIds, METRICS_FETCH_TIMEOUT_MS, METRIC_PREFIX_FILTER);
        lastRawMetricsFetchCostMs = Math.max(0, System.currentTimeMillis() - fetchStartMs);
        lastRawMetricsBlobs = rawMetrics == null ? 0 : rawMetrics.size();
        Map<Long, JobAgg> aggByJobId = aggregateRawMetrics(rawMetrics, enabledJobIds);
        for (Long jobId : enabledJobIds) {
            JobStore store = jobStores.get(jobId);
            if (store == null) {
                continue;
            }
            store.update(nowMs, aggByJobId.get(jobId));
        }
    }

    public List<Map<String, Object>> getJobsSummary() {
        return jobMetas.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(
                        e -> {
                            long jobId = e.getKey();
                            JobMeta meta = e.getValue();
                            JobStore store = jobStores.get(jobId);

                            Map<String, Object> m = new HashMap<>();
                            m.put(RestConstant.JOB_ID, String.valueOf(jobId));
                            m.put(RestConstant.JOB_NAME, meta.jobName);
                            m.put(RestConstant.JOB_STATUS, JobStatus.RUNNING.toString());
                            m.put(
                                    RestConstant.CREATE_TIME,
                                    DateTimeUtils.toString(
                                            meta.createTime,
                                            DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS));
                            if (store != null) {
                                m.putAll(store.getSummaryMetrics());
                            }
                            return m;
                        })
                .collect(Collectors.toList());
    }

    private ObservabilityConfig resolveObservabilityConfig(long jobId) {
        String jobName = String.valueOf(jobId);
        long createTime = System.currentTimeMillis();
        try {
            IMap<Long, JobInfo> runningJobInfoMap =
                    nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
            JobInfo runningJobInfo = runningJobInfoMap.get(jobId);
            if (runningJobInfo != null && runningJobInfo.getInitializationTimestamp() != null) {
                createTime = runningJobInfo.getInitializationTimestamp();
            }
        } catch (Exception ignored) {
            // ignore
        }
        try {
            JobDAGInfo jobInfo = coordinatorService.getJobInfo(jobId);
            if (jobInfo != null) {
                Map<String, Object> envOptions = jobInfo.getEnvOptions();
                ObservabilityConfig cfg =
                        ObservabilityConfig.fromEnvOptions(envOptions, jobName, createTime);
                return cfg;
            }
        } catch (Exception e) {
            log.debug(
                    "Resolve observability config from JobDAGInfo failed for job {}: {}",
                    jobId,
                    e.getMessage());
        }

        return ObservabilityConfig.disabled(jobName, createTime);
    }

    @SuppressWarnings("unchecked")
    private Set<Long> getRunningJobIds() {
        try {
            IMap<Object, Object> runningJobInfoMap =
                    (IMap<Object, Object>)
                            nodeEngine
                                    .getHazelcastInstance()
                                    .getMap(Constant.IMAP_RUNNING_JOB_INFO);
            return runningJobInfoMap.keySet().stream()
                    .filter(k -> k instanceof Long)
                    .map(k -> (Long) k)
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            return Collections.emptySet();
        }
    }

    private static final class JobMeta {
        private final boolean enabled;
        private final long bucketMs;
        private final int retentionMinutes;
        private final String jobName;
        private final long createTime;

        private JobMeta(
                boolean enabled,
                long bucketMs,
                int retentionMinutes,
                String jobName,
                long createTime) {
            this.enabled = enabled;
            this.bucketMs = bucketMs;
            this.retentionMinutes = retentionMinutes;
            this.jobName = jobName;
            this.createTime = createTime;
        }
    }

    private static final class JobStore {
        private final long bucketMs;
        private final int retentionMinutes;

        private volatile long latestBucketStartMs = -1;

        private final Deque<Bucket> buckets = new ArrayDeque<>();
        private final Map<Long, Long> lastBlockedNsSumByQueueId = new HashMap<>();
        private final Map<Long, Long> lastSourceReadNsSumById = new HashMap<>();
        private final Map<Long, Long> lastSourceIdleNsSumById = new HashMap<>();
        private final Map<Long, Long> lastTransformProcessNsSumById = new HashMap<>();
        private final Map<Long, Long> lastTransformRecordsInSumById = new HashMap<>();
        private final Map<Long, Long> lastTransformRecordsOutSumById = new HashMap<>();
        private final Map<Long, Long> lastSinkWriteNsSumById = new HashMap<>();
        private final Map<Long, Long> lastSinkRecordsInSumById = new HashMap<>();
        private final Map<Long, Long> lastSinkPrepareCommitNsSumById = new HashMap<>();
        private final Map<Long, Long> lastSinkCommitNsSumById = new HashMap<>();
        private final Map<Long, Long> lastSinkAbortNsSumById = new HashMap<>();

        private JobStore(long bucketMs, int retentionMinutes) {
            this.bucketMs = bucketMs;
            this.retentionMinutes = retentionMinutes;
        }

        private synchronized Map<String, Object> getSummaryMetrics() {
            Map<String, Object> m = new HashMap<>();
            Bucket latest = buckets.peekLast();
            if (latest != null) {
                m.put("recordsPerSecond", latest.recordsPerSecond);
                m.put("errorCount", latest.errorCount);
            }
            return m;
        }

        private synchronized void update(long nowMs, JobAgg jobAgg) {
            long bucketStartMs = (nowMs / bucketMs) * bucketMs;
            if (bucketStartMs != latestBucketStartMs) {
                latestBucketStartMs = bucketStartMs;
                buckets.addLast(new Bucket(bucketStartMs));
            }
            Bucket current = buckets.peekLast();
            if (current == null) {
                return;
            }

            updateEdges(jobAgg, current);
            updateVertices(jobAgg, current);
            evictOldBuckets(nowMs);
        }

        private void updateEdges(JobAgg jobAgg, Bucket current) {
            AggById blockedAgg = jobAgg == null ? AggById.EMPTY : jobAgg.queuePutBlockedNs;
            AggById sizeAgg = jobAgg == null ? AggById.EMPTY : jobAgg.queueSize;
            AggById capacityAgg = jobAgg == null ? AggById.EMPTY : jobAgg.queueCapacity;

            Set<Long> queueIds = new HashSet<>();
            queueIds.addAll(blockedAgg.sumById.keySet());
            queueIds.addAll(sizeAgg.sumById.keySet());
            queueIds.addAll(capacityAgg.sumById.keySet());

            for (Long queueId : queueIds) {
                MetricAgg blocked = blockedAgg.get(queueId);
                long currentSum = blocked.sum;
                long lastSum = lastBlockedNsSumByQueueId.getOrDefault(queueId, currentSum);
                long delta = Math.max(0, currentSum - lastSum);
                lastBlockedNsSumByQueueId.put(queueId, currentSum);

                MetricAgg size = sizeAgg.get(queueId);
                MetricAgg capacity = capacityAgg.get(queueId);

                long subtaskCount =
                        Math.max(1, Math.max(blocked.count, Math.max(size.count, capacity.count)));
                long queueSizeSum = size.sum;
                long capacitySum = capacity.sum;

                EdgeBucket previous = current.edges.get(queueId);
                long emitBlockedNs =
                        previous == null ? delta : Math.max(0, previous.emitBlockedNs) + delta;
                current.edges.put(
                        queueId,
                        new EdgeBucket(emitBlockedNs, subtaskCount, queueSizeSum, capacitySum));
            }
        }

        private void updateVertices(JobAgg jobAgg, Bucket current) {
            AggById sourceReadAgg = jobAgg == null ? AggById.EMPTY : jobAgg.sourceReadNs;
            AggById sourceIdleAgg = jobAgg == null ? AggById.EMPTY : jobAgg.sourceIdleNs;
            AggById transformProcessAgg =
                    jobAgg == null ? AggById.EMPTY : jobAgg.transformProcessNs;
            AggById transformRecordsInAgg =
                    jobAgg == null ? AggById.EMPTY : jobAgg.transformRecordsIn;
            AggById transformRecordsOutAgg =
                    jobAgg == null ? AggById.EMPTY : jobAgg.transformRecordsOut;
            AggById sinkWriteAgg = jobAgg == null ? AggById.EMPTY : jobAgg.sinkWriteNs;
            AggById sinkRecordsInAgg = jobAgg == null ? AggById.EMPTY : jobAgg.sinkRecordsIn;
            AggById sinkPrepareCommitAgg =
                    jobAgg == null ? AggById.EMPTY : jobAgg.sinkPrepareCommitNs;
            AggById sinkCommitAgg = jobAgg == null ? AggById.EMPTY : jobAgg.sinkCommitNs;
            AggById sinkAbortAgg = jobAgg == null ? AggById.EMPTY : jobAgg.sinkAbortNs;

            Set<Long> vertexIds = new HashSet<>();
            vertexIds.addAll(sourceReadAgg.sumById.keySet());
            vertexIds.addAll(sourceIdleAgg.sumById.keySet());
            vertexIds.addAll(transformProcessAgg.sumById.keySet());
            vertexIds.addAll(transformRecordsInAgg.sumById.keySet());
            vertexIds.addAll(transformRecordsOutAgg.sumById.keySet());
            vertexIds.addAll(sinkWriteAgg.sumById.keySet());
            vertexIds.addAll(sinkRecordsInAgg.sumById.keySet());
            vertexIds.addAll(sinkPrepareCommitAgg.sumById.keySet());
            vertexIds.addAll(sinkCommitAgg.sumById.keySet());
            vertexIds.addAll(sinkAbortAgg.sumById.keySet());

            for (Long vertexId : vertexIds) {
                VertexBucket previous = current.vertices.get(vertexId);
                VertexBucket merged = previous == null ? new VertexBucket() : previous.copy();

                MetricAgg readAgg = sourceReadAgg.get(vertexId);
                MetricAgg idleAgg = sourceIdleAgg.get(vertexId);
                MetricAgg tpAgg = transformProcessAgg.get(vertexId);
                MetricAgg triAgg = transformRecordsInAgg.get(vertexId);
                MetricAgg troAgg = transformRecordsOutAgg.get(vertexId);
                MetricAgg swAgg = sinkWriteAgg.get(vertexId);
                MetricAgg sriAgg = sinkRecordsInAgg.get(vertexId);
                MetricAgg spcAgg = sinkPrepareCommitAgg.get(vertexId);
                MetricAgg scAgg = sinkCommitAgg.get(vertexId);
                MetricAgg saAgg = sinkAbortAgg.get(vertexId);

                int subtaskCount =
                        Math.max(
                                Math.max(readAgg.count, idleAgg.count),
                                Math.max(
                                        Math.max(tpAgg.count, Math.max(triAgg.count, troAgg.count)),
                                        Math.max(
                                                Math.max(swAgg.count, sriAgg.count),
                                                Math.max(
                                                        Math.max(spcAgg.count, scAgg.count),
                                                        saAgg.count))));
                merged.subtaskCount = Math.max(merged.subtaskCount, subtaskCount);

                merged.sourceReadNs += deltaSum(vertexId, readAgg.sum, lastSourceReadNsSumById);
                merged.sourceIdleNs += deltaSum(vertexId, idleAgg.sum, lastSourceIdleNsSumById);
                merged.transformProcessNs +=
                        deltaSum(vertexId, tpAgg.sum, lastTransformProcessNsSumById);
                merged.transformRecordsIn +=
                        deltaSum(vertexId, triAgg.sum, lastTransformRecordsInSumById);
                merged.transformRecordsOut +=
                        deltaSum(vertexId, troAgg.sum, lastTransformRecordsOutSumById);
                merged.sinkWriteNs += deltaSum(vertexId, swAgg.sum, lastSinkWriteNsSumById);
                merged.sinkRecordsIn += deltaSum(vertexId, sriAgg.sum, lastSinkRecordsInSumById);
                merged.sinkPrepareCommitNs +=
                        deltaSum(vertexId, spcAgg.sum, lastSinkPrepareCommitNsSumById);
                merged.sinkCommitNs += deltaSum(vertexId, scAgg.sum, lastSinkCommitNsSumById);
                merged.sinkAbortNs += deltaSum(vertexId, saAgg.sum, lastSinkAbortNsSumById);

                current.vertices.put(vertexId, merged);
            }

            long totalRecords = 0;
            for (VertexBucket vb : current.vertices.values()) {
                totalRecords += vb.sinkRecordsIn;
            }
            current.recordsPerSecond =
                    totalRecords / ((bucketMs / 1000) > 0 ? (bucketMs / 1000) : 1);
        }

        private static long deltaSum(Long id, long currentSum, Map<Long, Long> lastSumById) {
            long lastSum = lastSumById.getOrDefault(id, currentSum);
            long delta = Math.max(0, currentSum - lastSum);
            lastSumById.put(id, currentSum);
            return delta;
        }

        private void evictOldBuckets(long nowMs) {
            long expireBeforeMs = nowMs - TimeUnit.MINUTES.toMillis(retentionMinutes);
            while (!buckets.isEmpty() && buckets.peekFirst().bucketStartMs < expireBeforeMs) {
                buckets.removeFirst();
            }
        }

        private synchronized Map<String, Object> toEdgesResponse(long windowMs) {
            long nowMs = System.currentTimeMillis();
            long requestedWindowMs = Math.max(0, windowMs);
            long retentionMs = TimeUnit.MINUTES.toMillis(retentionMinutes);
            long effectiveWindowMs = Math.min(requestedWindowMs, retentionMs);
            long fromMs = Math.max(0, nowMs - effectiveWindowMs);

            List<Bucket> selected =
                    buckets.stream()
                            .filter(b -> b.bucketStartMs >= fromMs)
                            .collect(Collectors.toList());

            Map<String, Object> resp = new HashMap<>();
            resp.put("bucketMs", bucketMs);
            resp.put("fromMs", fromMs);
            resp.put("toMs", nowMs);

            Map<Long, List<Map<String, Object>>> seriesByQueueId = new HashMap<>();
            for (Bucket b : selected) {
                for (Map.Entry<Long, EdgeBucket> e : b.edges.entrySet()) {
                    long queueId = e.getKey();
                    EdgeBucket eb = e.getValue();
                    seriesByQueueId
                            .computeIfAbsent(queueId, k -> new ArrayList<>())
                            .add(edgePoint(b.bucketStartMs, bucketMs, eb));
                }
            }

            List<Map<String, Object>> edges =
                    seriesByQueueId.entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .map(
                                    e -> {
                                        long queueId = e.getKey();
                                        Map<String, Object> m = new HashMap<>();
                                        m.put("queueId", queueId);
                                        m.put("targetVertexId", decodeQueueTargetVertexId(queueId));
                                        m.put("points", e.getValue());
                                        return m;
                                    })
                            .collect(Collectors.toList());
            resp.put("edges", edges);
            return resp;
        }

        private synchronized Map<String, Object> toVerticesResponse(long windowMs) {
            long nowMs = System.currentTimeMillis();
            long requestedWindowMs = Math.max(0, windowMs);
            long retentionMs = TimeUnit.MINUTES.toMillis(retentionMinutes);
            long effectiveWindowMs = Math.min(requestedWindowMs, retentionMs);
            long fromMs = Math.max(0, nowMs - effectiveWindowMs);

            List<Bucket> selected =
                    buckets.stream()
                            .filter(b -> b.bucketStartMs >= fromMs)
                            .collect(Collectors.toList());

            Map<String, Object> resp = new HashMap<>();
            resp.put("bucketMs", bucketMs);
            resp.put("fromMs", fromMs);
            resp.put("toMs", nowMs);

            Map<Long, List<Map<String, Object>>> seriesByVertexId = new HashMap<>();
            for (Bucket b : selected) {
                for (Map.Entry<Long, VertexBucket> e : b.vertices.entrySet()) {
                    long vertexId = e.getKey();
                    VertexBucket vb = e.getValue();
                    seriesByVertexId
                            .computeIfAbsent(vertexId, k -> new ArrayList<>())
                            .add(vertexPoint(b.bucketStartMs, bucketMs, vb));
                }
            }

            List<Map<String, Object>> vertices =
                    seriesByVertexId.entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .map(
                                    e -> {
                                        Map<String, Object> m = new HashMap<>();
                                        m.put("vertexId", e.getKey());
                                        m.put("points", e.getValue());
                                        return m;
                                    })
                            .collect(Collectors.toList());
            resp.put("vertices", vertices);
            return resp;
        }

        private static Map<String, Object> edgePoint(long ts, long bucketMs, EdgeBucket eb) {
            Map<String, Object> point = new HashMap<>();
            point.put("ts", ts);
            point.put("emitBlockedNs", eb.emitBlockedNs);
            point.put("subtaskCount", eb.subtaskCount);
            double bpRatio =
                    eb.subtaskCount <= 0
                            ? 0D
                            : ((double) eb.emitBlockedNs)
                                    / ((double) TimeUnit.MILLISECONDS.toNanos(bucketMs)
                                            * eb.subtaskCount);
            point.put("bpRatio", bpRatio);
            point.put("queueCapacity", eb.capacitySum);
            long queueSize = Math.max(0L, eb.queueSizeSum);
            if (eb.capacitySum > 0L) {
                // Guard against occasional metric sampling inconsistencies.
                queueSize = Math.min(queueSize, eb.capacitySum);
            }
            point.put("queueSize", queueSize);
            double fillRatio =
                    eb.capacitySum <= 0 ? 0D : ((double) queueSize) / ((double) eb.capacitySum);
            point.put("queueFillRatio", fillRatio);
            return point;
        }

        private static Map<String, Object> vertexPoint(long ts, long bucketMs, VertexBucket vb) {
            Map<String, Object> point = new HashMap<>();
            point.put("ts", ts);
            point.put("subtaskCount", vb.subtaskCount);

            long bucketNs = TimeUnit.MILLISECONDS.toNanos(bucketMs);
            long denom = bucketNs * Math.max(1, vb.subtaskCount);

            point.put("sourceReadNs", vb.sourceReadNs);
            point.put("sourceIdleNs", vb.sourceIdleNs);
            point.put("sourceReadRatio", ratio01(denom, vb.sourceReadNs));
            point.put("sourceIdleRatio", ratio01(denom, vb.sourceIdleNs));

            point.put("transformProcessNs", vb.transformProcessNs);
            point.put("transformRecordsIn", vb.transformRecordsIn);
            point.put("transformRecordsOut", vb.transformRecordsOut);
            point.put("transformBusyRatio", ratio01(denom, vb.transformProcessNs));
            point.put(
                    "transformProcessNsPerRecord",
                    vb.transformRecordsIn <= 0
                            ? 0D
                            : ((double) vb.transformProcessNs) / ((double) vb.transformRecordsIn));

            point.put("sinkWriteNs", vb.sinkWriteNs);
            point.put("sinkRecordsIn", vb.sinkRecordsIn);
            point.put("sinkPrepareCommitNs", vb.sinkPrepareCommitNs);
            point.put("sinkCommitNs", vb.sinkCommitNs);
            point.put("sinkAbortNs", vb.sinkAbortNs);
            point.put("sinkBusyRatio", ratio01(denom, vb.sinkWriteNs));
            point.put(
                    "sinkWriteNsPerRecord",
                    vb.sinkRecordsIn <= 0
                            ? 0D
                            : ((double) vb.sinkWriteNs) / ((double) vb.sinkRecordsIn));

            return point;
        }
    }

    private static double ratio01(long denom, long numer) {
        if (denom <= 0 || numer <= 0) {
            return 0D;
        }
        double r = ((double) numer) / ((double) denom);
        if (r < 0D) {
            return 0D;
        }
        if (r > 1D) {
            return 1D;
        }
        return r;
    }

    private static long decodeQueueTargetVertexId(long queueId) {
        // Queue IDs generated by PhysicalPlanGenerator:
        // - async boundary queue: -2 * actionId (negative even)
        // - sink split queue: -(2 * actionId + 1) (negative odd)
        // Decode them back to the downstream actionId so UI can correlate with job DAG vertexId.
        if (queueId >= 0) {
            return queueId;
        }
        long abs = Math.abs(queueId);
        if (abs == 0) {
            return -1;
        }
        if ((abs % 2) == 0) {
            return abs / 2;
        }
        return (abs - 1) / 2;
    }

    private static Map<String, Object> emptyEdgesResponse(
            long bucketMs, long windowMs, boolean enabled) {
        long nowMs = System.currentTimeMillis();
        long fromMs = Math.max(0, nowMs - Math.max(0, windowMs));
        Map<String, Object> resp = new HashMap<>();
        resp.put("enabled", enabled);
        resp.put("bucketMs", bucketMs);
        resp.put("fromMs", fromMs);
        resp.put("toMs", nowMs);
        resp.put("edges", Collections.emptyList());
        return resp;
    }

    private static Map<String, Object> emptyVerticesResponse(
            long bucketMs, long windowMs, boolean enabled) {
        long nowMs = System.currentTimeMillis();
        long fromMs = Math.max(0, nowMs - Math.max(0, windowMs));
        Map<String, Object> resp = new HashMap<>();
        resp.put("enabled", enabled);
        resp.put("bucketMs", bucketMs);
        resp.put("fromMs", fromMs);
        resp.put("toMs", nowMs);
        resp.put("vertices", Collections.emptyList());
        return resp;
    }

    private static final class Bucket {
        private final long bucketStartMs;
        private final Map<Long, EdgeBucket> edges = new HashMap<>();
        private final Map<Long, VertexBucket> vertices = new HashMap<>();

        private long recordsPerSecond;
        private long errorCount;

        private Bucket(long bucketStartMs) {
            this.bucketStartMs = bucketStartMs;
        }
    }

    private static final class EdgeBucket {
        private final long emitBlockedNs;
        private final long subtaskCount;
        private final long queueSizeSum;
        private final long capacitySum;

        private EdgeBucket(
                long emitBlockedNs, long subtaskCount, long queueSizeSum, long capacitySum) {
            this.emitBlockedNs = emitBlockedNs;
            this.subtaskCount = subtaskCount;
            this.queueSizeSum = queueSizeSum;
            this.capacitySum = capacitySum;
        }
    }

    private static final class VertexBucket {
        private long subtaskCount;

        private long sourceReadNs;
        private long sourceIdleNs;

        private long transformProcessNs;
        private long transformRecordsIn;
        private long transformRecordsOut;

        private long sinkWriteNs;
        private long sinkRecordsIn;
        private long sinkPrepareCommitNs;
        private long sinkCommitNs;
        private long sinkAbortNs;

        private VertexBucket copy() {
            VertexBucket b = new VertexBucket();
            b.subtaskCount = subtaskCount;
            b.sourceReadNs = sourceReadNs;
            b.sourceIdleNs = sourceIdleNs;
            b.transformProcessNs = transformProcessNs;
            b.transformRecordsIn = transformRecordsIn;
            b.transformRecordsOut = transformRecordsOut;
            b.sinkWriteNs = sinkWriteNs;
            b.sinkRecordsIn = sinkRecordsIn;
            b.sinkPrepareCommitNs = sinkPrepareCommitNs;
            b.sinkCommitNs = sinkCommitNs;
            b.sinkAbortNs = sinkAbortNs;
            return b;
        }
    }

    private static final class MetricAgg {
        private static final MetricAgg EMPTY = new MetricAgg(0L, 0);
        private final long sum;
        private final int count;

        private MetricAgg(long sum, int count) {
            this.sum = sum;
            this.count = count;
        }
    }

    private static Map<Long, JobAgg> aggregateRawMetrics(
            List<RawJobMetrics> rawMetrics, Set<Long> enabledJobIds) {
        if (rawMetrics == null || rawMetrics.isEmpty()) {
            return Collections.emptyMap();
        }
        RawAggConsumer consumer = new RawAggConsumer(enabledJobIds);
        for (RawJobMetrics metrics : rawMetrics) {
            if (metrics == null || metrics.getBlob() == null) {
                continue;
            }
            MetricsCompressor.extractMetrics(metrics.getBlob(), consumer);
        }
        return consumer.byJobId;
    }

    private static final class JobAgg {
        private final AggById queuePutBlockedNs = new AggById();
        private final AggById queueSize = new AggById();
        private final AggById queueCapacity = new AggById();

        private final AggById sourceReadNs = new AggById();
        private final AggById sourceIdleNs = new AggById();

        private final AggById transformProcessNs = new AggById();
        private final AggById transformRecordsIn = new AggById();
        private final AggById transformRecordsOut = new AggById();

        private final AggById sinkWriteNs = new AggById();
        private final AggById sinkRecordsIn = new AggById();
        private final AggById sinkPrepareCommitNs = new AggById();
        private final AggById sinkCommitNs = new AggById();
        private final AggById sinkAbortNs = new AggById();

        private AggById select(String base) {
            switch (base) {
                case INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS:
                    return queuePutBlockedNs;
                case INTERMEDIATE_QUEUE_SIZE:
                    return queueSize;
                case INTERMEDIATE_QUEUE_CAPACITY:
                    return queueCapacity;
                case SOURCE_READ_NANOS:
                    return sourceReadNs;
                case SOURCE_IDLE_NANOS:
                    return sourceIdleNs;
                case TRANSFORM_PROCESS_NANOS:
                    return transformProcessNs;
                case TRANSFORM_RECORDS_IN:
                    return transformRecordsIn;
                case TRANSFORM_RECORDS_OUT:
                    return transformRecordsOut;
                case SINK_WRITE_NANOS:
                    return sinkWriteNs;
                case SINK_RECORDS_IN:
                    return sinkRecordsIn;
                case SINK_PREPARE_COMMIT_NANOS:
                    return sinkPrepareCommitNs;
                case SINK_COMMIT_NANOS:
                    return sinkCommitNs;
                case SINK_ABORT_NANOS:
                    return sinkAbortNs;
                default:
                    return null;
            }
        }
    }

    private static final class AggById {
        private static final AggById EMPTY = new AggById(true);

        private final Map<Long, Long> sumById;
        private final Map<Long, Integer> countById;

        private AggById() {
            this(false);
        }

        private AggById(boolean immutableEmpty) {
            if (immutableEmpty) {
                this.sumById = Collections.emptyMap();
                this.countById = Collections.emptyMap();
            } else {
                this.sumById = new HashMap<>();
                this.countById = new HashMap<>();
            }
        }

        private void add(long id, long value) {
            sumById.merge(id, value, Long::sum);
            countById.merge(id, 1, Integer::sum);
        }

        private MetricAgg get(long id) {
            long sum = sumById.getOrDefault(id, 0L);
            int count = countById.getOrDefault(id, 0);
            if (sum == 0L && count == 0) {
                return MetricAgg.EMPTY;
            }
            return new MetricAgg(sum, count);
        }
    }

    private static final class RawAggConsumer implements MetricConsumer {
        private final Set<Long> enabledJobIds;
        private final Map<Long, JobAgg> byJobId = new HashMap<>();

        private RawAggConsumer(Set<Long> enabledJobIds) {
            this.enabledJobIds = enabledJobIds;
        }

        private static final class MetricId {
            private final long jobId;
            private final String base;
            private final long id;

            private MetricId(long jobId, String base, long id) {
                this.jobId = jobId;
                this.base = base;
                this.id = id;
            }
        }

        private static MetricId parseMetricId(MetricDescriptor descriptor) {
            if (descriptor == null) {
                return null;
            }
            String jobIdStr = descriptor.tagValue(JOB_ID_TAG);
            if (jobIdStr == null) {
                return null;
            }
            long jobId;
            try {
                jobId = Long.parseLong(jobIdStr);
            } catch (Exception e) {
                return null;
            }

            String metricName = descriptor.metric();
            if (metricName == null) {
                return null;
            }
            int idx = metricName.lastIndexOf('#');
            if (idx <= 0 || idx >= metricName.length() - 1) {
                return null;
            }
            String base = metricName.substring(0, idx);
            long id;
            try {
                id = Long.parseLong(metricName.substring(idx + 1));
            } catch (Exception e) {
                return null;
            }
            return new MetricId(jobId, base, id);
        }

        @Override
        public void consumeLong(MetricDescriptor descriptor, long value) {
            MetricId metricId = parseMetricId(descriptor);
            if (metricId == null) {
                return;
            }
            if (enabledJobIds != null && !enabledJobIds.contains(metricId.jobId)) {
                return;
            }

            JobAgg jobAgg = byJobId.computeIfAbsent(metricId.jobId, k -> new JobAgg());
            AggById target = jobAgg.select(metricId.base);
            if (target == null) {
                return;
            }
            target.add(metricId.id, value);
        }

        @Override
        public void consumeDouble(MetricDescriptor descriptor, double value) {
            // ignore
        }
    }
}
