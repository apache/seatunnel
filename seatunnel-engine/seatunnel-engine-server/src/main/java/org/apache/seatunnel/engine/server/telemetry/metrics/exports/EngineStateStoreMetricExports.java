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
import org.apache.seatunnel.engine.server.telemetry.metrics.AbstractCollector;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import io.prometheus.client.GaugeMetricFamily;

import java.util.Arrays;
import java.util.List;

public class EngineStateStoreMetricExports extends AbstractCollector {

    private static final String BACKEND = "hazelcast";

    private static final List<String> ENGINE_STATE_STORES =
            Arrays.asList(
                    Constant.IMAP_RUNNING_JOB_INFO,
                    Constant.IMAP_RUNNING_JOB_STATE,
                    Constant.IMAP_STATE_TIMESTAMPS,
                    Constant.IMAP_OWNED_SLOT_PROFILES,
                    Constant.IMAP_RUNNING_JOB_METRICS,
                    Constant.IMAP_FINISHED_JOB_STATE,
                    Constant.IMAP_FINISHED_JOB_METRICS,
                    Constant.IMAP_FINISHED_JOB_VERTEX_INFO,
                    Constant.IMAP_CHECKPOINT_MONITOR,
                    Constant.IMAP_CONNECTOR_JAR_REF_COUNTERS,
                    Constant.IMAP_CHECKPOINT_ID,
                    Constant.IMAP_PENDING_PIPELINE_CLEANUP);

    public EngineStateStoreMetricExports(Node node) {
        super(node);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<String> localLabelNames = clusterLabelNames(ADDRESS, "store", "backend");
        GaugeMetricFamily localOwnedEntries =
                new GaugeMetricFamily(
                        "engine_state_store_local_owned_entries",
                        "Local owned entries of an engine state store on this node",
                        localLabelNames);
        GaugeMetricFamily localBackupEntries =
                new GaugeMetricFamily(
                        "engine_state_store_local_backup_entries",
                        "Local backup entries of an engine state store on this node",
                        localLabelNames);
        GaugeMetricFamily localHeapCostBytes =
                new GaugeMetricFamily(
                        "engine_state_store_local_heap_cost_bytes",
                        "Local heap cost in bytes of an engine state store on this node",
                        localLabelNames);

        for (String storeName : ENGINE_STATE_STORES) {
            collectStoreMetrics(
                    storeName, localOwnedEntries, localBackupEntries, localHeapCostBytes);
        }

        return Arrays.asList(localOwnedEntries, localBackupEntries, localHeapCostBytes);
    }

    private void collectStoreMetrics(
            String storeName,
            GaugeMetricFamily localOwnedEntries,
            GaugeMetricFamily localBackupEntries,
            GaugeMetricFamily localHeapCostBytes) {
        try {
            IMap<?, ?> map = getNode().hazelcastInstance.getMap(storeName);
            LocalMapStats localMapStats = map.getLocalMapStats();
            List<String> localLabelValues = labelValues(localAddress(), storeName, BACKEND);
            localOwnedEntries.addMetric(localLabelValues, localMapStats.getOwnedEntryCount());
            localBackupEntries.addMetric(localLabelValues, localMapStats.getBackupEntryCount());
            localHeapCostBytes.addMetric(localLabelValues, localMapStats.getHeapCost());
        } catch (HazelcastInstanceNotActiveException e) {
            getLogger(getClass())
                    .fine(
                            String.format(
                                    "Skip state store metrics because Hazelcast is not active, store=%s, backend=%s",
                                    storeName, BACKEND),
                            e);
        } catch (Exception e) {
            getLogger(getClass())
                    .warning(
                            String.format(
                                    "Failed to collect state store metrics, store=%s, backend=%s",
                                    storeName, BACKEND),
                            e);
        }
    }
}
