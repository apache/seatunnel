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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

@DisabledOnOs(OS.WINDOWS)
class EngineStateStoreMetricExportsTest {

    private HazelcastInstanceImpl instance;

    @AfterEach
    void afterEach() {
        if (instance != null) {
            instance.shutdown();
        }
    }

    @Test
    void collectShouldExportLocalStateStoreMetrics() {
        instance =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName("EngineStateStoreMetricExportsTest_localMetrics"));
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertTrue(instance.node.isMaster()));
        instance.getMap(Constant.IMAP_RUNNING_JOB_INFO).put(1L, "job-info");

        List<MetricFamilySamples> metrics =
                new EngineStateStoreMetricExports(instance.node).collect();

        Sample ownedEntries =
                findSample(
                        metrics,
                        "engine_state_store_local_owned_entries",
                        Constant.IMAP_RUNNING_JOB_INFO);
        Assertions.assertEquals(
                Arrays.asList("cluster", "address", "store", "backend"), ownedEntries.labelNames);
        Assertions.assertEquals("hazelcast", labelValue(ownedEntries, "backend"));
        Assertions.assertFalse(labelValue(ownedEntries, "address").isEmpty());

        Assertions.assertNotNull(
                findSample(
                        metrics,
                        "engine_state_store_local_backup_entries",
                        Constant.IMAP_RUNNING_JOB_INFO));
        Assertions.assertNotNull(
                findSample(
                        metrics,
                        "engine_state_store_local_heap_cost_bytes",
                        Constant.IMAP_RUNNING_JOB_INFO));
    }

    @Test
    void collectShouldCoverAllEngineStateStores() {
        instance =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName(
                                "EngineStateStoreMetricExportsTest_allStateStores"));
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertTrue(instance.node.isMaster()));

        List<MetricFamilySamples> metrics =
                new EngineStateStoreMetricExports(instance.node).collect();

        Set<String> exportedStores =
                findMetricFamily(metrics, "engine_state_store_local_owned_entries").samples.stream()
                        .map(sample -> labelValue(sample, "store"))
                        .collect(Collectors.toSet());

        Assertions.assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                Constant.IMAP_RUNNING_JOB_INFO,
                                Constant.IMAP_RUNNING_JOB_STATE,
                                Constant.IMAP_STATE_TIMESTAMPS,
                                Constant.IMAP_OWNED_SLOT_PROFILES,
                                EngineStateStoreNames.RUNNING_JOB_METRICS,
                                Constant.IMAP_FINISHED_JOB_STATE,
                                Constant.IMAP_FINISHED_JOB_METRICS,
                                Constant.IMAP_FINISHED_JOB_VERTEX_INFO,
                                EngineStateStoreNames.CHECKPOINT_MONITOR,
                                Constant.IMAP_CONNECTOR_JAR_REF_COUNTERS,
                                Constant.IMAP_CHECKPOINT_ID,
                                Constant.IMAP_PENDING_PIPELINE_CLEANUP)),
                exportedStores);
    }

    private static MetricFamilySamples findMetricFamily(
            List<MetricFamilySamples> metrics, String name) {
        return metrics.stream()
                .filter(metricFamilySamples -> name.equals(metricFamilySamples.name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing metric family: " + name));
    }

    private static Sample findSample(
            List<MetricFamilySamples> metrics, String metricName, String storeName) {
        return findMetricFamily(metrics, metricName).samples.stream()
                .filter(sample -> storeName.equals(labelValue(sample, "store")))
                .findFirst()
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        "Missing metric sample: "
                                                + metricName
                                                + ", store="
                                                + storeName));
    }

    private static String labelValue(Sample sample, String labelName) {
        int index = sample.labelNames.indexOf(labelName);
        if (index < 0) {
            throw new AssertionError("Missing label: " + labelName);
        }
        return sample.labelValues.get(index);
    }
}
