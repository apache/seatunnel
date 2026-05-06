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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.org.apache.commons.lang3.ArrayUtils;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.metrics.MetricTags;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.ExecutionAddress;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.VertexInfo;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.dag.DAGUtils;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.operation.CancelJobOperation;
import org.apache.seatunnel.engine.server.operation.GetClusterHealthMetricsOperation;
import org.apache.seatunnel.engine.server.operation.GetJobMetricsOperation;
import org.apache.seatunnel.engine.server.operation.GetJobStatusOperation;
import org.apache.seatunnel.engine.server.operation.SavePointJobOperation;
import org.apache.seatunnel.engine.server.operation.SubmitJobOperation;
import org.apache.seatunnel.engine.server.rest.RestConstant;
import org.apache.seatunnel.engine.server.rest.RestJobExecutionEnvironment;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;
import org.apache.seatunnel.engine.server.utils.RestUtil;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.JsonUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.seatunnel.api.common.metrics.MetricNames.INTERMEDIATE_QUEUE_SIZE;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_COMMITTED_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_COMMITTED_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_COMMITTED_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_COMMITTED_QPS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_QPS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_QPS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_COMMITTED_BYTES;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_COMMITTED_BYTES_PER_SECONDS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_COMMITTED_COUNT;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_COMMITTED_QPS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_WRITE_BYTES;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_WRITE_BYTES_PER_SECONDS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_WRITE_COUNT;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_WRITE_QPS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SOURCE_RECEIVED_BYTES;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SOURCE_RECEIVED_BYTES_PER_SECONDS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SOURCE_RECEIVED_QPS;

@Slf4j
public abstract class BaseService {

    private static final int JOB_METRICS_LOG_TRUNCATE_LENGTH = 500;
    private static final Pattern VERTEX_IDENTIFIER_PATTERN =
            Pattern.compile("((?:Sink|Source|Transform)\\[(\\d+)\\])");

    protected final NodeEngineImpl nodeEngine;

    public BaseService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    protected SeaTunnelServer getSeaTunnelServer(boolean shouldBeMaster) {
        Map<String, Object> extensionServices =
                nodeEngine.getNode().getNodeExtension().createExtensionServices();
        SeaTunnelServer seaTunnelServer =
                (SeaTunnelServer) extensionServices.get(Constant.SEATUNNEL_SERVICE_NAME);
        if (shouldBeMaster && !seaTunnelServer.isMasterNode()) {
            return null;
        }
        return seaTunnelServer;
    }

    protected JsonObject convertToJson(JobInfo jobInfo, long jobId) {

        JsonObject jobInfoJson = new JsonObject();
        JobImmutableInformation jobImmutableInformation =
                nodeEngine
                        .getSerializationService()
                        .toObject(
                                nodeEngine
                                        .getSerializationService()
                                        .toObject(jobInfo.getJobImmutableInformation()));

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);
        ClassLoaderService classLoaderService =
                seaTunnelServer == null
                        ? getSeaTunnelServer(false).getClassLoaderService()
                        : seaTunnelServer.getClassLoaderService();
        LogicalDag logicalDag =
                DAGUtils.restoreLogicalDag(
                        jobImmutableInformation,
                        nodeEngine.getSerializationService(),
                        classLoaderService);

        String jobMetrics;
        JobStatus jobStatus;
        if (seaTunnelServer == null) {
            jobMetrics =
                    (String)
                            NodeEngineUtil.sendOperationToMasterNode(
                                            nodeEngine, new GetJobMetricsOperation(jobId))
                                    .join();
            jobStatus =
                    JobStatus.values()[
                            (int)
                                    NodeEngineUtil.sendOperationToMasterNode(
                                                    nodeEngine, new GetJobStatusOperation(jobId))
                                            .join()];
        } else {
            jobMetrics =
                    seaTunnelServer.getCoordinatorService().getJobMetrics(jobId).toJsonString();
            jobStatus = seaTunnelServer.getCoordinatorService().getJobStatus(jobId);
        }

        JobDAGInfo jobDAGInfo =
                DAGUtils.getJobDAGInfo(
                        logicalDag,
                        jobImmutableInformation,
                        getSeaTunnelServer(false).getSeaTunnelConfig().getEngineConfig(),
                        true,
                        new ExecutionAddress(
                                this.nodeEngine.getMasterAddress().getHost(),
                                this.nodeEngine.getMasterAddress().getPort()),
                        new HashSet<>());

        jobInfoJson
                .add(RestConstant.JOB_ID, String.valueOf(jobId))
                .add(RestConstant.JOB_NAME, logicalDag.getJobConfig().getName())
                .add(RestConstant.JOB_STATUS, jobStatus.toString())
                .add(
                        RestConstant.ENV_OPTIONS,
                        JsonUtil.toJsonObject(logicalDag.getJobConfig().getEnvOptions()))
                .add(
                        RestConstant.CREATE_TIME,
                        DateTimeUtils.toString(
                                jobImmutableInformation.getCreateTime(),
                                DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS))
                .add(RestConstant.START_TIME, getJobStartTime(jobId))
                .add(
                        RestConstant.JOB_DAG,
                        jobDAGInfo != null ? jobDAGInfo.toJsonObject() : new JsonObject())
                .add(
                        RestConstant.PLUGIN_JARS_URLS,
                        (JsonValue)
                                jobImmutableInformation.getPluginJarsUrls().stream()
                                        .map(
                                                url -> {
                                                    JsonObject jarUrl = new JsonObject();
                                                    jarUrl.add(
                                                            RestConstant.JAR_PATH, url.toString());
                                                    return jarUrl;
                                                })
                                        .collect(JsonArray::new, JsonArray::add, JsonArray::add))
                .add(
                        RestConstant.IS_START_WITH_SAVE_POINT,
                        jobImmutableInformation.isStartWithSavePoint())
                .add(
                        RestConstant.METRICS,
                        metricsToJsonObject(getJobMetrics(jobMetrics, jobDAGInfo)));

        return jobInfoJson;
    }

    private String getJobStartTime(long jobId) {
        IMap<Object, Long[]> stateTimestamps =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        Long[] jobStateTimestamps = stateTimestamps.get(jobId);
        if (jobStateTimestamps != null) {
            Long startTimestamp = jobStateTimestamps[JobStatus.SCHEDULED.ordinal()];
            if (startTimestamp != null) {
                return DateTimeUtils.toString(
                        startTimestamp, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
            }
        }
        return "";
    }

    protected JsonObject getJobInfoJson(
            JobHistoryService.JobState jobState, String jobMetrics, JobDAGInfo jobDAGInfo) {
        return new JsonObject()
                .add(RestConstant.JOB_ID, String.valueOf(jobState.getJobId()))
                .add(RestConstant.JOB_NAME, jobState.getJobName())
                .add(RestConstant.JOB_STATUS, jobState.getJobStatus().toString())
                .add(RestConstant.ERROR_MSG, jobState.getErrorMessage())
                .add(
                        RestConstant.CREATE_TIME,
                        DateTimeUtils.toString(
                                jobState.getSubmitTime(),
                                DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS))
                .add(
                        RestConstant.START_TIME,
                        jobState.getStartTime() == null
                                ? ""
                                : DateTimeUtils.toString(
                                        jobState.getStartTime(),
                                        DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS))
                .add(
                        RestConstant.FINISH_TIME,
                        jobState.getFinishTime() == null
                                ? ""
                                : DateTimeUtils.toString(
                                        jobState.getFinishTime(),
                                        DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS))
                .add(
                        RestConstant.JOB_DAG,
                        jobDAGInfo != null ? jobDAGInfo.toJsonObject() : new JsonObject())
                .add(RestConstant.PLUGIN_JARS_URLS, new JsonArray())
                .add(
                        RestConstant.METRICS,
                        metricsToJsonObject(getJobMetrics(jobMetrics, jobDAGInfo)));
    }

    private Map<String, Object> getJobMetrics(String jobMetrics, JobDAGInfo jobDAGInfo) {
        Map<String, Object> metricsMap = new HashMap<>();

        Map<String, List<String>> tableToSourceIdentifiersMap = new HashMap<>();
        Map<String, List<String>> tableToSinkIdentifiersMap = new HashMap<>();
        if (jobDAGInfo != null && jobDAGInfo.getVertexInfoMap() != null) {
            for (VertexInfo vertexInfo : jobDAGInfo.getVertexInfoMap().values()) {
                String identifier = extractVertexIdentifier(vertexInfo.getConnectorType());
                if (vertexInfo.getTablePaths() == null
                        || identifier.equals(vertexInfo.getConnectorType())) {
                    continue;
                }
                Map<String, List<String>> targetMap = null;
                if (vertexInfo.getType() == PluginType.SOURCE) {
                    targetMap = tableToSourceIdentifiersMap;
                } else if (vertexInfo.getType() == PluginType.SINK) {
                    targetMap = tableToSinkIdentifiersMap;
                }

                if (targetMap != null) {
                    for (TablePath tablePath : vertexInfo.getTablePaths()) {
                        targetMap
                                .computeIfAbsent(tablePath.getFullName(), k -> new ArrayList<>())
                                .add(identifier);
                    }
                }
            }
            sortVertexIdentifiers(tableToSourceIdentifiersMap);
            sortVertexIdentifiers(tableToSinkIdentifiersMap);
        }

        // To add metrics, populate the corresponding array,
        String[] countMetricsNames = {
            SOURCE_RECEIVED_COUNT,
            SINK_WRITE_COUNT,
            SINK_COMMITTED_COUNT,
            SOURCE_RECEIVED_BYTES,
            SINK_WRITE_BYTES,
            SINK_COMMITTED_BYTES,
            INTERMEDIATE_QUEUE_SIZE
        };
        String[] rateMetricsNames = {
            SOURCE_RECEIVED_QPS,
            SINK_WRITE_QPS,
            SINK_COMMITTED_QPS,
            SOURCE_RECEIVED_BYTES_PER_SECONDS,
            SINK_WRITE_BYTES_PER_SECONDS,
            SINK_COMMITTED_BYTES_PER_SECONDS
        };
        String[] tableCountMetricsNames = {
            TABLE_SOURCE_RECEIVED_COUNT,
            TABLE_SINK_WRITE_COUNT,
            TABLE_SINK_COMMITTED_COUNT,
            TABLE_SOURCE_RECEIVED_BYTES,
            TABLE_SINK_WRITE_BYTES,
            TABLE_SINK_COMMITTED_BYTES
        };
        String[] tableRateMetricsNames = {
            TABLE_SOURCE_RECEIVED_QPS,
            TABLE_SINK_WRITE_QPS,
            TABLE_SINK_COMMITTED_QPS,
            TABLE_SOURCE_RECEIVED_BYTES_PER_SECONDS,
            TABLE_SINK_WRITE_BYTES_PER_SECONDS,
            TABLE_SINK_COMMITTED_BYTES_PER_SECONDS
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
                    new HashMap<>(), // Sink Committed Count
                    new HashMap<>(), // Source Received Bytes
                    new HashMap<>(), // Sink Write Bytes
                    new HashMap<>(), // Sink Committed Bytes
                    new HashMap<>(), // Source Received QPS
                    new HashMap<>(), // Sink Write QPS
                    new HashMap<>(), // Sink Committed QPS
                    new HashMap<>(), // Source Received Bytes Per Second
                    new HashMap<>(), // Sink Write Bytes Per Second
                    new HashMap<>() // Sink Committed Bytes Per Second
                };

        try {
            JsonNode jobMetricsStr = new ObjectMapper().readTree(jobMetrics);

            jobMetricsStr
                    .fieldNames()
                    .forEachRemaining(
                            metricName -> {
                                if (!metricName.contains("#")) {
                                    return;
                                }
                                try {
                                    String tableName =
                                            TablePath.of(metricName.split("#")[1]).getFullName();
                                    JsonNode metricNode = jobMetricsStr.get(metricName);

                                    Map<String, java.util.List<String>> identifiersMap = null;
                                    if (metricName.startsWith("TableSource")
                                            || metricName.startsWith("Source")) {
                                        identifiersMap = tableToSourceIdentifiersMap;
                                    } else if (metricName.startsWith("TableSink")
                                            || metricName.startsWith("Sink")) {
                                        identifiersMap = tableToSinkIdentifiersMap;
                                    }

                                    processMetric(
                                            metricName,
                                            tableName,
                                            metricNode,
                                            tableMetricsMaps,
                                            identifiersMap);
                                } catch (Exception e) {
                                    log.error(
                                            "Failed to process metric '{}': {}. Continuing with other metrics.",
                                            metricName,
                                            e.getMessage(),
                                            e);
                                }
                            });

            // Aggregation summary and rate metrics
            aggregateMetrics(
                    jobMetricsStr,
                    metricsSums,
                    metricsRates,
                    ArrayUtils.addAll(countMetricsNames, rateMetricsNames));

        } catch (JsonProcessingException e) {
            log.error(
                    "Failed to parse job metrics JSON: {}. Raw input (first {} chars): {}",
                    e.getMessage(),
                    JOB_METRICS_LOG_TRUNCATE_LENGTH,
                    truncateJobMetricsForLog(jobMetrics),
                    e);
            return metricsMap;
        } catch (Exception e) {
            log.error("Unexpected error while processing job metrics: {}", e.getMessage(), e);
            return metricsMap;
        }

        populateMetricsMap(
                metricsMap,
                tableMetricsMaps,
                ArrayUtils.addAll(tableCountMetricsNames, tableRateMetricsNames),
                tableCountMetricsNames.length);
        populateMetricsMap(
                metricsMap,
                Stream.concat(Arrays.stream(metricsSums), Arrays.stream(metricsRates))
                        .toArray(Number[]::new),
                ArrayUtils.addAll(countMetricsNames, rateMetricsNames),
                metricsSums.length);

        return metricsMap;
    }

    private void processMetric(
            String metricName,
            String tableName,
            JsonNode metricNode,
            Map<String, JsonNode>[] tableMetricsMaps,
            Map<String, java.util.List<String>> tableToVertexIdentifiersMap) {
        if (metricNode == null) {
            return;
        }

        List<String> vertexIdentifiers =
                tableToVertexIdentifiersMap == null
                        ? null
                        : tableToVertexIdentifiersMap.get(tableName);

        if (vertexIdentifiers == null || vertexIdentifiers.isEmpty()) {
            putMetricToMap(metricName, tableName, metricNode, tableMetricsMaps);
            return;
        }

        if (!metricNode.isArray()) {
            String metricKey = tableName;
            if (vertexIdentifiers.size() == 1) {
                metricKey = vertexIdentifiers.get(0) + "." + tableName;
            } else {
                log.warn(
                        "Cannot reliably determine vertex assignment for table '{}' metric '{}' (isArray=false) with {} configured vertices, using table name only to avoid incorrect attribution",
                        tableName,
                        metricName,
                        vertexIdentifiers.size());
            }
            putMetricToMap(metricName, metricKey, metricNode, tableMetricsMaps);
            return;
        }

        // Prefer tag-based attribution to handle partial/mismatched arrays reliably.
        ObjectMapper mapper = new ObjectMapper();
        Map<String, ArrayNode> metricsByIdentifier = new HashMap<>();
        ArrayNode unassignedMetrics = null;
        for (JsonNode node : metricNode) {
            String identifier = extractVertexIdentifierFromMetricNode(node);
            if (StringUtils.isNotBlank(identifier) && vertexIdentifiers.contains(identifier)) {
                metricsByIdentifier
                        .computeIfAbsent(identifier, k -> mapper.createArrayNode())
                        .add(node);
            } else {
                if (unassignedMetrics == null) {
                    unassignedMetrics = mapper.createArrayNode();
                }
                unassignedMetrics.add(node);
            }
        }

        if (!metricsByIdentifier.isEmpty()) {
            metricsByIdentifier.keySet().stream()
                    .sorted(vertexIdentifierComparator())
                    .forEach(
                            identifier -> {
                                putMetricToMap(
                                        metricName,
                                        identifier + "." + tableName,
                                        metricsByIdentifier.get(identifier),
                                        tableMetricsMaps);
                            });

            if (vertexIdentifiers.size() > 1
                    && metricsByIdentifier.size() < vertexIdentifiers.size()) {
                log.warn(
                        "Some vertices may not be reporting metrics yet for table '{}': expected {} vertices {}, but only received metrics for {} vertices {}",
                        tableName,
                        vertexIdentifiers.size(),
                        vertexIdentifiers,
                        metricsByIdentifier.size(),
                        metricsByIdentifier.keySet());
            }

            if (unassignedMetrics != null && unassignedMetrics.size() > 0) {
                log.warn(
                        "Found {} unassigned metric entries for table '{}' metric '{}', using table name key only for these entries",
                        unassignedMetrics.size(),
                        tableName,
                        metricName);
                putMetricToMap(metricName, tableName, unassignedMetrics, tableMetricsMaps);
            }
            return;
        }

        // Fallback for legacy/simplified metric nodes without tags (mainly in tests or older
        // outputs).
        int arraySize = metricNode.size();
        if (vertexIdentifiers.size() > 1) {
            if (arraySize == vertexIdentifiers.size()) {
                for (int i = 0; i < arraySize; i++) {
                    String identifier = vertexIdentifiers.get(i);
                    String metricKey = identifier + "." + tableName;
                    JsonNode element = metricNode.get(i);
                    if (element != null && element.isArray()) {
                        putMetricToMap(metricName, metricKey, element, tableMetricsMaps);
                    } else {
                        ArrayNode wrapped = mapper.createArrayNode();
                        wrapped.add(element);
                        putMetricToMap(metricName, metricKey, wrapped, tableMetricsMaps);
                    }
                }
            } else if (arraySize > 0 && arraySize < vertexIdentifiers.size()) {
                log.warn(
                        "Metric array size mismatch for table '{}': expected {} vertices {} but got {} metric entries. Some vertices may not be reporting metrics yet.",
                        tableName,
                        vertexIdentifiers.size(),
                        vertexIdentifiers,
                        arraySize);
                for (int i = 0; i < arraySize; i++) {
                    String identifier = vertexIdentifiers.get(i);
                    String metricKey = identifier + "." + tableName;
                    JsonNode element = metricNode.get(i);
                    if (element != null && element.isArray()) {
                        putMetricToMap(metricName, metricKey, element, tableMetricsMaps);
                    } else {
                        ArrayNode wrapped = mapper.createArrayNode();
                        wrapped.add(element);
                        putMetricToMap(metricName, metricKey, wrapped, tableMetricsMaps);
                    }
                }
            } else if (arraySize > vertexIdentifiers.size()) {
                log.error(
                        "Invalid metric array size for table '{}': received {} metric entries but only {} vertices {} configured. Using table name only.",
                        tableName,
                        arraySize,
                        vertexIdentifiers.size(),
                        vertexIdentifiers);
                putMetricToMap(metricName, tableName, metricNode, tableMetricsMaps);
            } else {
                log.warn(
                        "Metric array size mismatch for table '{}': expected {} vertices {} but got {} metric entries. Using table name only to avoid incorrect attribution.",
                        tableName,
                        vertexIdentifiers.size(),
                        vertexIdentifiers,
                        arraySize);
                putMetricToMap(metricName, tableName, metricNode, tableMetricsMaps);
            }
            return;
        }

        // Single vertex: safe to prefix.
        String metricKey = vertexIdentifiers.get(0) + "." + tableName;
        putMetricToMap(metricName, metricKey, metricNode, tableMetricsMaps);
    }

    private void putMetricToMap(
            String metricName,
            String metricKey,
            JsonNode metricNode,
            Map<String, JsonNode>[] tableMetricsMaps) {

        // Define index constant
        final int SOURCE_COUNT_IDX = 0,
                SINK_COUNT_IDX = 1,
                SINK_COMMITTED_COUNT_IDX = 2,
                SOURCE_BYTES_IDX = 3,
                SINK_BYTES_IDX = 4,
                SINK_COMMITTED_BYTES_IDX = 5,
                SOURCE_QPS_IDX = 6,
                SINK_QPS_IDX = 7,
                SINK_COMMITTED_QPS_IDX = 8,
                SOURCE_BYTES_SEC_IDX = 9,
                SINK_BYTES_SEC_IDX = 10,
                SINK_COMMITTED_BYTES_SEC_IDX = 11;
        if (metricName.startsWith(SOURCE_RECEIVED_COUNT + "#")) {
            tableMetricsMaps[SOURCE_COUNT_IDX].put(metricKey, metricNode);
        } else if (metricName.startsWith(SINK_WRITE_COUNT + "#")) {
            tableMetricsMaps[SINK_COUNT_IDX].put(metricKey, metricNode);
        } else if (metricName.startsWith(SINK_COMMITTED_COUNT + "#")) {
            tableMetricsMaps[SINK_COMMITTED_COUNT_IDX].put(metricKey, metricNode);
        } else if (metricName.startsWith(SOURCE_RECEIVED_BYTES + "#")) {
            tableMetricsMaps[SOURCE_BYTES_IDX].put(metricKey, metricNode);
        } else if (metricName.startsWith(SINK_WRITE_BYTES + "#")) {
            tableMetricsMaps[SINK_BYTES_IDX].put(metricKey, metricNode);
        } else if (metricName.startsWith(SINK_COMMITTED_BYTES + "#")) {
            tableMetricsMaps[SINK_COMMITTED_BYTES_IDX].put(metricKey, metricNode);
        } else if (metricName.startsWith(SOURCE_RECEIVED_QPS + "#")) {
            tableMetricsMaps[SOURCE_QPS_IDX].put(metricKey, metricNode);
        } else if (metricName.startsWith(SINK_WRITE_QPS + "#")) {
            tableMetricsMaps[SINK_QPS_IDX].put(metricKey, metricNode);
        } else if (metricName.startsWith(SINK_COMMITTED_QPS + "#")) {
            tableMetricsMaps[SINK_COMMITTED_QPS_IDX].put(metricKey, metricNode);
        } else if (metricName.startsWith(SOURCE_RECEIVED_BYTES_PER_SECONDS + "#")) {
            tableMetricsMaps[SOURCE_BYTES_SEC_IDX].put(metricKey, metricNode);
        } else if (metricName.startsWith(SINK_WRITE_BYTES_PER_SECONDS + "#")) {
            tableMetricsMaps[SINK_BYTES_SEC_IDX].put(metricKey, metricNode);
        } else if (metricName.startsWith(SINK_COMMITTED_BYTES_PER_SECONDS + "#")) {
            tableMetricsMaps[SINK_COMMITTED_BYTES_SEC_IDX].put(metricKey, metricNode);
        }
    }

    private String extractVertexIdentifier(String vertexName) {
        if (StringUtils.isBlank(vertexName)) {
            return "";
        }

        Matcher matcher = VERTEX_IDENTIFIER_PATTERN.matcher(vertexName);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return vertexName;
    }

    private String extractVertexIdentifierFromMetricNode(JsonNode metricNode) {
        if (metricNode == null) {
            return "";
        }
        JsonNode tagsNode = metricNode.path("tags");
        if (tagsNode.isMissingNode() || !tagsNode.isObject()) {
            return "";
        }
        String taskName = tagsNode.path(MetricTags.TASK_NAME).asText("");
        if (StringUtils.isBlank(taskName)) {
            return "";
        }
        Matcher matcher = VERTEX_IDENTIFIER_PATTERN.matcher(taskName);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    private Comparator<String> vertexIdentifierComparator() {
        return Comparator.comparingInt(this::vertexIdentifierIndex)
                .thenComparing(Comparator.naturalOrder());
    }

    private int vertexIdentifierIndex(String identifier) {
        if (StringUtils.isBlank(identifier)) {
            return Integer.MAX_VALUE;
        }
        Matcher matcher = VERTEX_IDENTIFIER_PATTERN.matcher(identifier);
        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(2));
            } catch (NumberFormatException ignored) {
                return Integer.MAX_VALUE;
            }
        }
        return Integer.MAX_VALUE;
    }

    private void sortVertexIdentifiers(Map<String, List<String>> tableToVertexIdentifiersMap) {
        if (tableToVertexIdentifiersMap == null || tableToVertexIdentifiersMap.isEmpty()) {
            return;
        }
        tableToVertexIdentifiersMap
                .values()
                .forEach(
                        identifiers -> {
                            identifiers.sort(vertexIdentifierComparator());
                        });
    }

    private String truncateJobMetricsForLog(String jobMetrics) {
        if (jobMetrics == null) {
            return "null";
        }
        if (jobMetrics.length() > JOB_METRICS_LOG_TRUNCATE_LENGTH) {
            return jobMetrics.substring(0, JOB_METRICS_LOG_TRUNCATE_LENGTH) + "...";
        }
        return jobMetrics;
    }

    private void aggregateMetrics(
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

    private void populateMetricsMap(
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

    private Map<String, Object> aggregateMap(Map<String, JsonNode> inputMap, boolean isRate) {
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
                                                                node -> node.path("value").asLong())
                                                        .sum()));
    }

    private JsonObject metricsToJsonObject(Map<String, Object> jobMetrics) {
        JsonObject members = new JsonObject();
        jobMetrics.forEach(
                (key, value) -> {
                    if (value instanceof Map) {
                        members.add(key, metricsToJsonObject((Map<String, Object>) value));
                    } else {
                        String strValue;
                        if (value instanceof Float
                                || value instanceof Double
                                || value instanceof BigDecimal) {
                            strValue = new BigDecimal(value.toString()).toPlainString();
                        } else {
                            strValue = value.toString();
                        }
                        members.add(key, strValue);
                    }
                });
        return members;
    }

    protected JsonNode requestHandle(byte[] requestBody) {
        if (requestBody.length == 0) {
            throw new IllegalArgumentException("Request body is empty.");
        }
        JsonNode requestBodyJsonNode;
        try {
            requestBodyJsonNode = RestUtil.convertByteToJsonNode(requestBody);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid JSON format in request body.");
        }
        return requestBodyJsonNode;
    }

    protected void handleStopJob(
            Map<String, Object> map, SeaTunnelServer seaTunnelServer, Node node) {
        boolean isStopWithSavePoint = false;
        if (map.get(RestConstant.JOB_ID) == null) {
            throw new IllegalArgumentException("jobId cannot be empty.");
        }
        long jobId = Long.parseLong(map.get(RestConstant.JOB_ID).toString());
        if (map.get(RestConstant.IS_STOP_WITH_SAVE_POINT) != null) {
            isStopWithSavePoint =
                    Boolean.parseBoolean(map.get(RestConstant.IS_STOP_WITH_SAVE_POINT).toString());
        }
        boolean forceStop = false;
        if (map.get(RestConstant.FORCE) != null) {
            forceStop = Boolean.parseBoolean(map.get(RestConstant.FORCE).toString());
        }

        if (!seaTunnelServer.isMasterNode()) {
            if (forceStop) {
                NodeEngineUtil.sendOperationToMasterNode(
                                node.nodeEngine, new CancelJobOperation(jobId, true))
                        .join();
                return;
            }
            if (isStopWithSavePoint) {
                NodeEngineUtil.sendOperationToMasterNode(
                                node.nodeEngine, new SavePointJobOperation(jobId))
                        .join();
            } else {
                NodeEngineUtil.sendOperationToMasterNode(
                                node.nodeEngine, new CancelJobOperation(jobId, false))
                        .join();
            }

        } else {
            CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();
            if (forceStop) {
                coordinatorService.stopJob(jobId);
                return;
            }
            if (isStopWithSavePoint) {
                coordinatorService.savePoint(jobId);
            } else {
                coordinatorService.cancelJob(jobId);
            }
        }
    }

    protected String mapToUrlParams(Map<String, String> params) {
        return params.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&", "?", ""));
    }

    protected JsonObject submitJobInternal(
            Config config,
            Map<String, String> requestParams,
            SeaTunnelServer seaTunnelServer,
            Node node) {
        ReadonlyConfig envOptions = ReadonlyConfig.fromConfig(config.getConfig("env"));
        String jobName = envOptions.get(EnvCommonOptions.JOB_NAME);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(
                StringUtils.isEmpty(requestParams.get(RestConstant.JOB_NAME))
                        ? jobName
                        : requestParams.get(RestConstant.JOB_NAME));

        boolean startWithSavePoint =
                Boolean.parseBoolean(requestParams.get(RestConstant.IS_START_WITH_SAVE_POINT));
        String jobIdStr = requestParams.get(RestConstant.JOB_ID);
        Long finalJobId = StringUtils.isNotBlank(jobIdStr) ? Long.parseLong(jobIdStr) : null;
        RestJobExecutionEnvironment restJobExecutionEnvironment =
                new RestJobExecutionEnvironment(
                        seaTunnelServer, jobConfig, config, node, startWithSavePoint, finalJobId);
        JobImmutableInformation jobImmutableInformation = restJobExecutionEnvironment.build();
        long jobId = jobImmutableInformation.getJobId();
        if (!seaTunnelServer.isMasterNode()) {

            NodeEngineUtil.sendOperationToMasterNode(
                            node.nodeEngine,
                            new SubmitJobOperation(
                                    jobId,
                                    node.nodeEngine.toData(jobImmutableInformation),
                                    jobImmutableInformation.isStartWithSavePoint()))
                    .join();

        } else {
            submitJob(node, seaTunnelServer, jobImmutableInformation, jobConfig);
        }

        return new JsonObject()
                .add(RestConstant.JOB_ID, String.valueOf(jobId))
                .add(RestConstant.JOB_NAME, jobConfig.getName());
    }

    private void submitJob(
            Node node,
            SeaTunnelServer seaTunnelServer,
            JobImmutableInformation jobImmutableInformation,
            JobConfig jobConfig) {
        CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();
        Data data = node.nodeEngine.getSerializationService().toData(jobImmutableInformation);
        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                coordinatorService.submitJob(
                        Long.parseLong(jobConfig.getJobContext().getJobId()),
                        data,
                        jobImmutableInformation.isStartWithSavePoint());
        voidPassiveCompletableFuture.join();
    }

    protected JsonArray getSystemMonitoringInformationJsonValues() {
        Cluster cluster = nodeEngine.getHazelcastInstance().getCluster();

        Set<Member> members = cluster.getMembers();
        JsonArray jsonValues =
                members.stream()
                        .map(
                                member -> {
                                    Address address = member.getAddress();
                                    String input = null;
                                    try {
                                        input =
                                                (String)
                                                        NodeEngineUtil.sendOperationToMemberNode(
                                                                        nodeEngine,
                                                                        new GetClusterHealthMetricsOperation(),
                                                                        address)
                                                                .get();
                                    } catch (InterruptedException | ExecutionException e) {

                                        log.error("Failed to get cluster health metrics", e);
                                    }
                                    String[] parts = input.split(", ");
                                    JsonObject jobInfo = new JsonObject();
                                    Arrays.stream(parts)
                                            .forEach(
                                                    part -> {
                                                        String[] keyValue = part.split("=");
                                                        jobInfo.add(keyValue[0], keyValue[1]);
                                                    });
                                    return jobInfo;
                                })
                        .collect(JsonArray::new, JsonArray::add, JsonArray::add);
        return jsonValues;
    }
}
