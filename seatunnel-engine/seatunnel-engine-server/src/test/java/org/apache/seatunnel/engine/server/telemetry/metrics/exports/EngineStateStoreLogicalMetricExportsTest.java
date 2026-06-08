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

package org.apache.seatunnel.engine.server.telemetry.metrics.exports;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.ConnectorJarType;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.RefCount;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCloseReason;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.JobHistoryService.JobState;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@DisabledOnOs(OS.WINDOWS)
class EngineStateStoreLogicalMetricExportsTest {

    private HazelcastInstanceImpl instance;

    @AfterEach
    void afterEach() {
        if (instance != null) {
            instance.shutdown();
        }
    }

    @Test
    void collectShouldExportLogicalMetricsForSpecialStateStores() {
        instance = SeaTunnelServerStarter.createHazelcastInstance(createTestConfig());
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertTrue(instance.node.isMaster()));

        SeaTunnelServer server =
                instance.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertTrue(server.isCoordinatorActive()));

        seedRunningJobMetrics();
        seedCheckpointMonitor(server);
        seedFinishedJobStores();
        seedConnectorJarRefCounters();

        List<MetricFamilySamples> metrics =
                new EngineStateStoreLogicalMetricExports(instance.node).collect();

        Assertions.assertEquals(
                3d,
                findSampleValue(
                        metrics, "engine_state_store_running_job_metrics_task_contexts", null));
        Assertions.assertEquals(
                2d,
                findSampleValue(
                        metrics,
                        "engine_state_store_running_job_metrics_active_partition_keys",
                        null));
        Assertions.assertEquals(
                1d, findSampleValue(metrics, "engine_state_store_checkpoint_monitor_jobs", null));
        Assertions.assertEquals(
                1d,
                findSampleValue(
                        metrics,
                        "engine_state_store_checkpoint_monitor_in_progress_checkpoints",
                        null));
        Assertions.assertEquals(
                2d,
                findSampleValue(
                        metrics,
                        "engine_state_store_checkpoint_monitor_retained_history_entries",
                        null));
        Assertions.assertEquals(
                1d,
                findSampleValue(
                        metrics,
                        "engine_state_store_finished_job_records",
                        Constant.IMAP_FINISHED_JOB_STATE));
        Assertions.assertEquals(
                1d,
                findSampleValue(
                        metrics,
                        "engine_state_store_finished_job_records",
                        Constant.IMAP_FINISHED_JOB_METRICS));
        Assertions.assertEquals(
                1d,
                findSampleValue(
                        metrics,
                        "engine_state_store_finished_job_records",
                        Constant.IMAP_FINISHED_JOB_VERTEX_INFO));
        Assertions.assertEquals(
                1d,
                findSampleValue(metrics, "engine_state_store_connector_jar_tracked_jars", null));
        Assertions.assertEquals(
                2d,
                findSampleValue(
                        metrics, "engine_state_store_connector_jar_total_references", null));
    }

    @Test
    void collectShouldTrackFinishedJobCleanupTotals() {
        instance = SeaTunnelServerStarter.createHazelcastInstance(createTestConfig());
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertTrue(instance.node.isMaster()));
        SeaTunnelServer server =
                instance.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertTrue(server.isCoordinatorActive()));

        IMap<Long, JobState> finishedJobStateMap =
                instance.getMap(Constant.IMAP_FINISHED_JOB_STATE);
        IMap<Long, JobMetrics> finishedJobMetricsMap =
                instance.getMap(Constant.IMAP_FINISHED_JOB_METRICS);
        IMap<Long, JobDAGInfo> finishedJobVertexInfoMap =
                instance.getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO);
        finishedJobStateMap.put(101L, createJobState(101L), 1, TimeUnit.MILLISECONDS);
        finishedJobMetricsMap.put(101L, JobMetrics.of(new HashMap<>()), 1, TimeUnit.MILLISECONDS);
        finishedJobVertexInfoMap.put(101L, createJobDagInfo(101L), 1, TimeUnit.MILLISECONDS);

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<MetricFamilySamples> metrics =
                                    new EngineStateStoreLogicalMetricExports(instance.node)
                                            .collect();
                            Assertions.assertEquals(
                                    1d,
                                    findSampleValue(
                                            metrics,
                                            "engine_state_store_finished_job_cleanup_total",
                                            Constant.IMAP_FINISHED_JOB_STATE));
                            Assertions.assertEquals(
                                    1d,
                                    findSampleValue(
                                            metrics,
                                            "engine_state_store_finished_job_cleanup_total",
                                            Constant.IMAP_FINISHED_JOB_METRICS));
                            Assertions.assertEquals(
                                    1d,
                                    findSampleValue(
                                            metrics,
                                            "engine_state_store_finished_job_cleanup_total",
                                            Constant.IMAP_FINISHED_JOB_VERTEX_INFO));
                        });
    }

    private void seedRunningJobMetrics() {
        IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsMap =
                instance.getMap(Constant.IMAP_RUNNING_JOB_METRICS);
        HashMap<TaskLocation, SeaTunnelMetricsContext> partitionZero = new HashMap<>();
        partitionZero.put(new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 0, 0), metricCtx());
        partitionZero.put(new TaskLocation(new TaskGroupLocation(1L, 1, 2L), 0, 0), metricCtx());
        HashMap<TaskLocation, SeaTunnelMetricsContext> partitionOne = new HashMap<>();
        partitionOne.put(new TaskLocation(new TaskGroupLocation(2L, 1, 1L), 0, 0), metricCtx());
        metricsMap.put(0L, partitionZero);
        metricsMap.put(1L, partitionOne);
    }

    private void seedCheckpointMonitor(SeaTunnelServer server) {
        server.getCheckpointMonitorService()
                .onCheckpointTriggered(7L, 1, 1001L, CheckpointType.CHECKPOINT_TYPE, 100L, 2);
        server.getCheckpointMonitorService()
                .onCheckpointCompleted(
                        new CompletedCheckpoint(
                                7L,
                                1,
                                1000L,
                                10L,
                                CheckpointType.CHECKPOINT_TYPE,
                                20L,
                                Collections.emptyMap(),
                                Collections.emptyMap()),
                        128L);
        server.getCheckpointMonitorService()
                .onCheckpointFailed(
                        7L,
                        1,
                        1002L,
                        CheckpointType.CHECKPOINT_TYPE,
                        CheckpointCloseReason.CHECKPOINT_EXPIRED,
                        null,
                        30L);
    }

    private void seedFinishedJobStores() {
        instance.getMap(Constant.IMAP_FINISHED_JOB_STATE).put(7L, createJobState(7L));
        instance.getMap(Constant.IMAP_FINISHED_JOB_METRICS).put(7L, JobMetrics.of(new HashMap<>()));
        instance.getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO).put(7L, createJobDagInfo(7L));
    }

    private void seedConnectorJarRefCounters() {
        IMap<ConnectorJarIdentifier, RefCount> connectorJarRefCounters =
                instance.getMap(Constant.IMAP_CONNECTOR_JAR_REF_COUNTERS);
        connectorJarRefCounters.put(
                ConnectorJarIdentifier.of(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR, "a.jar", "/tmp/a.jar"),
                createRefCount(2L));
    }

    private static SeaTunnelMetricsContext metricCtx() {
        return new SeaTunnelMetricsContext();
    }

    private static RefCount createRefCount(long references) {
        RefCount refCount = new RefCount();
        refCount.setReferences(references);
        return refCount;
    }

    private static JobState createJobState(long jobId) {
        return new JobState(
                jobId, "job-" + jobId, JobStatus.FINISHED, 1L, 2L, 3L, new HashMap<>(), null);
    }

    private static JobDAGInfo createJobDagInfo(long jobId) {
        return new JobDAGInfo(
                jobId,
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                null,
                Collections.emptySet());
    }

    private static double findSampleValue(
            List<MetricFamilySamples> metrics, String metricName, String storeName) {
        Sample sample =
                findMetricFamily(metrics, metricName).samples.stream()
                        .filter(
                                metricSample ->
                                        storeName == null
                                                || storeName.equals(
                                                        labelValue(metricSample, "store")))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Missing metric sample: "
                                                        + metricName
                                                        + ", store="
                                                        + storeName));
        Assertions.assertEquals("hazelcast", labelValue(sample, "backend"));
        return sample.value;
    }

    private static MetricFamilySamples findMetricFamily(
            List<MetricFamilySamples> metrics, String name) {
        return metrics.stream()
                .filter(
                        metricFamilySamples ->
                                name.equals(metricFamilySamples.name)
                                        || metricFamilySamples.samples.stream()
                                                .anyMatch(sample -> name.equals(sample.name)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing metric family: " + name));
    }

    private static String labelValue(Sample sample, String labelName) {
        int index = sample.labelNames.indexOf(labelName);
        if (index < 0) {
            throw new AssertionError(
                    "Missing label: "
                            + labelName
                            + ", labels="
                            + Arrays.toString(sample.labelNames.toArray()));
        }
        return sample.labelValues.get(index);
    }

    private static SeaTunnelConfig createTestConfig() {
        String yaml =
                "seatunnel:\n"
                        + "  engine:\n"
                        + "    telemetry:\n"
                        + "      metric:\n"
                        + "        enabled: true\n"
                        + "    history-job-expire-minutes: 1\n"
                        + "    jar-storage:\n"
                        + "      enable: true\n"
                        + "      connector-jar-storage-mode: SHARED\n"
                        + "      connector-jar-storage-path: \"\"\n"
                        + "      connector-jar-cleanup-task-interval: 3600\n"
                        + "      connector-jar-expiry-time: 600\n";
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfigFromString(yaml);
        config.getHazelcastConfig()
                .setClusterName(
                        TestUtils.getClusterName(
                                "EngineStateStoreLogicalMetricExportsTest_" + System.nanoTime()));
        return config;
    }
}
