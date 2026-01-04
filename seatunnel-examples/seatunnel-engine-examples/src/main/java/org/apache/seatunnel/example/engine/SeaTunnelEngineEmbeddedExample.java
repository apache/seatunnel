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

package org.apache.seatunnel.example.engine;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.org.apache.commons.lang3.ArrayUtils;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestJobExecutionEnvironment;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Embed the Zeta engine as a tool in the project. Start a local cluster instance in Cluster mode and use it to submit tasks, manage tasks, and query task metrics, etc. */
public class SeaTunnelEngineEmbeddedExample implements AutoCloseable {

    public static void main(String[] args) {
        try (SeaTunnelEngineEmbeddedExample server = new SeaTunnelEngineEmbeddedExample(); ) {
            server.start();
            String json =
                    "{\r\n"
                            + "    \"env\": {\r\n"
                            + "        \"job.mode\": \"batch\"\r\n"
                            + "    },\r\n"
                            + "    \"source\": [\r\n"
                            + "        {\r\n"
                            + "            \"plugin_name\": \"FakeSource\",\r\n"
                            + "            \"plugin_output\": \"fake\",\r\n"
                            + "            \"row.num\": 100,\r\n"
                            + "            \"schema\": {\r\n"
                            + "                \"fields\": {\r\n"
                            + "                    \"name\": \"string\",\r\n"
                            + "                    \"age\": \"int\",\r\n"
                            + "                    \"card\": \"int\"\r\n"
                            + "                }\r\n"
                            + "            }\r\n"
                            + "        }\r\n"
                            + "    ],\r\n"
                            + "    \"transform\": [\r\n"
                            + "    ],\r\n"
                            + "    \"sink\": [\r\n"
                            + "        {\r\n"
                            + "            \"plugin_name\": \"Console\",\r\n"
                            + "            \"plugin_input\": [\"fake\"]\r\n"
                            + "        }\r\n"
                            + "    ]\r\n"
                            + "}";
            long jobId = server.submitJob("demo", json);
            System.err.println("jobId: " + jobId);

            JobResult jobResult = server.waitForJobComplete(jobId);
            System.err.println("------------jobResult-------------");
            System.err.println("jobStatus: " + jobResult.getStatus());
            System.err.println("jobError: " + jobResult.getError());

            Map<String, Object> jobMetricsSummary = server.getJobMetricsSummary(jobId);
            System.err.println("------------jobMetricsSummary-------------");
            System.err.println(JsonUtils.toJsonString(jobMetricsSummary));
        }
    }

    private HazelcastInstanceImpl hazelcastInstance;
    private SeaTunnelServer seaTunnelServer;

    public synchronized void start() {
        if (this.seaTunnelServer != null) {
            return;
        }
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getEngineConfig()
                .setClusterRole(EngineConfig.ClusterRole.MASTER_AND_WORKER);
        HazelcastInstanceImpl hazelcastInstance =
                SeaTunnelServerStarter.createHazelcastInstance(
                        seaTunnelConfig, "Zeta-" + UUID.randomUUID());
        this.hazelcastInstance = hazelcastInstance;

        Map<String, Object> extensionServices =
                hazelcastInstance.node.getNodeExtension().createExtensionServices();
        SeaTunnelServer seaTunnelServer =
                (SeaTunnelServer) extensionServices.get(Constant.SEATUNNEL_SERVICE_NAME);
        this.seaTunnelServer = seaTunnelServer;
    }

    public synchronized void close() {
        if (this.hazelcastInstance != null) {
            this.hazelcastInstance.shutdown();
            this.hazelcastInstance = null;
            this.seaTunnelServer = null;
        }
    }

    public long submitJob(String jobName, String seatunnelJobConfig) {
        return this.submitJob(jobName, JsonUtils.parseObject(seatunnelJobConfig));
    }

    public long submitJob(String jobName, ObjectNode seatunnelJobConfig) {
        return this.submitJob(jobName, JsonUtils.toMap(seatunnelJobConfig));
    }

    public long submitJob(String jobName, Map<String, Object> seatunnelJobConfig) {
        Config config = ConfigBuilder.of(seatunnelJobConfig);
        ReadonlyConfig envOptions = ReadonlyConfig.fromConfig(config.getConfig("env"));
        String nameFromConfig = envOptions.get(EnvCommonOptions.JOB_NAME);
        if (nameFromConfig != null) {
            jobName = nameFromConfig;
        }
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        RestJobExecutionEnvironment restJobExecutionEnvironment =
                new RestJobExecutionEnvironment(
                        seaTunnelServer,
                        jobConfig,
                        config,
                        this.hazelcastInstance.node,
                        false,
                        null);
        JobImmutableInformation jobImmutableInformation = restJobExecutionEnvironment.build();
        long jobId = jobImmutableInformation.getJobId();
        CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();
        Data data =
                this.hazelcastInstance
                        .node
                        .nodeEngine
                        .getSerializationService()
                        .toData(jobImmutableInformation);
        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                coordinatorService.submitJob(
                        Long.parseLong(jobConfig.getJobContext().getJobId()),
                        data,
                        jobImmutableInformation.isStartWithSavePoint());
        voidPassiveCompletableFuture.join();
        return jobId;
    }

    public JobResult waitForJobComplete(long jobId) {
        CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();
        PassiveCompletableFuture<JobResult> waitForJobCompleteFuture =
                coordinatorService.waitForJobComplete(jobId);
        JobResult jobResult = waitForJobCompleteFuture.join();
        return jobResult;
    }

    public Map<String, Object> getJobMetricsSummary(long jobId) {
        CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();
        JobMetrics jobMetrics = coordinatorService.getJobMetrics(jobId);
        return JobMetricsParser.getJobMetrics(jobMetrics);
    }

    public static class JobMetricsParser {
        public static final String SOURCE_RECEIVED_COUNT = "SourceReceivedCount";
        public static final String SOURCE_RECEIVED_BYTES = "SourceReceivedBytes";
        public static final String SOURCE_RECEIVED_QPS = "SourceReceivedQPS";
        public static final String SOURCE_RECEIVED_BYTES_PER_SECONDS =
                "SourceReceivedBytesPerSeconds";
        public static final String SINK_WRITE_COUNT = "SinkWriteCount";
        public static final String SINK_WRITE_BYTES = "SinkWriteBytes";
        public static final String SINK_WRITE_QPS = "SinkWriteQPS";
        public static final String SINK_WRITE_BYTES_PER_SECONDS = "SinkWriteBytesPerSeconds";

        public static final String INTERMEDIATE_QUEUE_SIZE = "IntermediateQueueSize";

        public static final String TABLE_SOURCE_RECEIVED_COUNT = "TableSourceReceivedCount";
        public static final String TABLE_SINK_WRITE_COUNT = "TableSinkWriteCount";
        public static final String TABLE_SOURCE_RECEIVED_QPS = "TableSourceReceivedQPS";
        public static final String TABLE_SINK_WRITE_QPS = "TableSinkWriteQPS";
        public static final String TABLE_SOURCE_RECEIVED_BYTES = "TableSourceReceivedBytes";
        public static final String TABLE_SINK_WRITE_BYTES = "TableSinkWriteBytes";
        public static final String TABLE_SOURCE_RECEIVED_BYTES_PER_SECONDS =
                "TableSourceReceivedBytesPerSeconds";
        public static final String TABLE_SINK_WRITE_BYTES_PER_SECONDS =
                "TableSinkWriteBytesPerSeconds";

        public static Map<String, Object> getJobMetrics(JobMetrics jobMetrics) {
            Map<String, Object> metricsMap = new HashMap<>();
            // To add metrics, populate the corresponding array,
            String[] countMetricsNames = {
                SOURCE_RECEIVED_COUNT,
                SINK_WRITE_COUNT,
                SOURCE_RECEIVED_BYTES,
                SINK_WRITE_BYTES,
                INTERMEDIATE_QUEUE_SIZE
            };
            String[] rateMetricsNames = {
                SOURCE_RECEIVED_QPS,
                SINK_WRITE_QPS,
                SOURCE_RECEIVED_BYTES_PER_SECONDS,
                SINK_WRITE_BYTES_PER_SECONDS
            };
            String[] tableCountMetricsNames = {
                TABLE_SOURCE_RECEIVED_COUNT,
                TABLE_SINK_WRITE_COUNT,
                TABLE_SOURCE_RECEIVED_BYTES,
                TABLE_SINK_WRITE_BYTES
            };
            String[] tableRateMetricsNames = {
                TABLE_SOURCE_RECEIVED_QPS,
                TABLE_SINK_WRITE_QPS,
                TABLE_SOURCE_RECEIVED_BYTES_PER_SECONDS,
                TABLE_SINK_WRITE_BYTES_PER_SECONDS
            };
            Long[] metricsSums =
                    Stream.generate(() -> 0L).limit(countMetricsNames.length).toArray(Long[]::new);
            Double[] metricsRates =
                    Stream.generate(() -> 0D).limit(rateMetricsNames.length).toArray(Double[]::new);

            // Used to store various indicators at the table
            Map<String, JsonNode>[] tableMetricsMaps =
                    new Map[] {
                        new HashMap<>(), // Source Received Count
                        new HashMap<>(), // Sink Write Count
                        new HashMap<>(), // Source Received Bytes
                        new HashMap<>(), // Sink Write Bytes
                        new HashMap<>(), // Source Received QPS
                        new HashMap<>(), // Sink Write QPS
                        new HashMap<>(), // Source Received Bytes Per Second
                        new HashMap<>() // Sink Write Bytes Per Second
                    };
            try {
                JsonNode jobMetricsStr = new ObjectMapper().readTree(jobMetrics.toJsonString());

                jobMetricsStr
                        .fieldNames()
                        .forEachRemaining(
                                metricName -> {
                                    if (metricName.contains("#")) {
                                        String tableName =
                                                TablePath.of(metricName.split("#")[1])
                                                        .getFullName();
                                        JsonNode metricNode = jobMetricsStr.get(metricName);
                                        processMetric(
                                                metricName,
                                                tableName,
                                                metricNode,
                                                tableMetricsMaps);
                                    }
                                });

                // Aggregation summary and rate metrics
                aggregateMetrics(
                        jobMetricsStr,
                        metricsSums,
                        metricsRates,
                        ArrayUtils.addAll(countMetricsNames, rateMetricsNames));

            } catch (JsonProcessingException e) {
                return metricsMap;
            }

            populateMetricsMap(
                    metricsMap,
                    tableMetricsMaps,
                    ArrayUtils.addAll(tableCountMetricsNames, tableRateMetricsNames),
                    countMetricsNames.length);
            populateMetricsMap(
                    metricsMap,
                    Stream.concat(Arrays.stream(metricsSums), Arrays.stream(metricsRates))
                            .toArray(Number[]::new),
                    ArrayUtils.addAll(countMetricsNames, rateMetricsNames),
                    metricsSums.length);

            return metricsMap;
        }

        private static void processMetric(
                String metricName,
                String tableName,
                JsonNode metricNode,
                Map<String, JsonNode>[] tableMetricsMaps) {
            if (metricNode == null) {
                return;
            }

            // Define index constant
            final int SOURCE_COUNT_IDX = 0,
                    SINK_COUNT_IDX = 1,
                    SOURCE_BYTES_IDX = 2,
                    SINK_BYTES_IDX = 3,
                    SOURCE_QPS_IDX = 4,
                    SINK_QPS_IDX = 5,
                    SOURCE_BYTES_SEC_IDX = 6,
                    SINK_BYTES_SEC_IDX = 7;
            if (metricName.startsWith(SOURCE_RECEIVED_COUNT + "#")) {
                tableMetricsMaps[SOURCE_COUNT_IDX].put(tableName, metricNode);
            } else if (metricName.startsWith(SINK_WRITE_COUNT + "#")) {
                tableMetricsMaps[SINK_COUNT_IDX].put(tableName, metricNode);
            } else if (metricName.startsWith(SOURCE_RECEIVED_BYTES + "#")) {
                tableMetricsMaps[SOURCE_BYTES_IDX].put(tableName, metricNode);
            } else if (metricName.startsWith(SINK_WRITE_BYTES + "#")) {
                tableMetricsMaps[SINK_BYTES_IDX].put(tableName, metricNode);
            } else if (metricName.startsWith(SOURCE_RECEIVED_QPS + "#")) {
                tableMetricsMaps[SOURCE_QPS_IDX].put(tableName, metricNode);
            } else if (metricName.startsWith(SINK_WRITE_QPS + "#")) {
                tableMetricsMaps[SINK_QPS_IDX].put(tableName, metricNode);
            } else if (metricName.startsWith(SOURCE_RECEIVED_BYTES_PER_SECONDS + "#")) {
                tableMetricsMaps[SOURCE_BYTES_SEC_IDX].put(tableName, metricNode);
            } else if (metricName.startsWith(SINK_WRITE_BYTES_PER_SECONDS + "#")) {
                tableMetricsMaps[SINK_BYTES_SEC_IDX].put(tableName, metricNode);
            }
        }

        private static void aggregateMetrics(
                JsonNode jobMetricsStr,
                Long[] metricsSums,
                Double[] metricsRates,
                String[] metricsNames) {
            for (int i = 0; i < metricsNames.length; i++) {
                JsonNode metricNode = jobMetricsStr.get(metricsNames[i]);
                if (metricNode != null && metricNode.isArray()) {
                    for (JsonNode node : metricNode) {
                        // Match Rate Metrics vs. Value Metrics
                        if (i < metricsSums.length) {
                            metricsSums[i] += node.path("value").asLong();
                        } else {
                            metricsRates[i - metricsSums.length] += node.path("value").asDouble();
                        }
                    }
                }
            }
        }

        private static void populateMetricsMap(
                Map<String, Object> metricsMap,
                Object[] metrics,
                String[] metricNames,
                int countMetricNames) {
            for (int i = 0; i < metrics.length; i++) {
                if (metrics[i] != null) {
                    if (metrics[i] instanceof Map) {
                        metricsMap.put(
                                metricNames[i],
                                aggregateMap(
                                        (Map<String, JsonNode>) metrics[i], i >= countMetricNames));
                    } else {
                        metricsMap.put(metricNames[i], metrics[i]);
                    }
                }
            }
        }

        private static Map<String, Object> aggregateMap(
                Map<String, JsonNode> inputMap, boolean isRate) {
            return isRate
                    ? inputMap.entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry ->
                                                    StreamSupport.stream(
                                                                    entry.getValue().spliterator(),
                                                                    false)
                                                            .mapToDouble(
                                                                    node ->
                                                                            node.path("value")
                                                                                    .asDouble())
                                                            .sum()))
                    : inputMap.entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry ->
                                                    StreamSupport.stream(
                                                                    entry.getValue().spliterator(),
                                                                    false)
                                                            .mapToLong(
                                                                    node ->
                                                                            node.path("value")
                                                                                    .asLong())
                                                            .sum()));
        }
    }
}
