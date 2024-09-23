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

package org.apache.seatunnel.engine.server.rest;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.env.EnvironmentUtil;
import org.apache.seatunnel.engine.common.env.Version;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.log.Log4j2HttpGetCommandProcessor;
import org.apache.seatunnel.engine.server.master.JobHistoryService.JobState;
import org.apache.seatunnel.engine.server.operation.GetClusterHealthMetricsOperation;
import org.apache.seatunnel.engine.server.operation.GetJobMetricsOperation;
import org.apache.seatunnel.engine.server.operation.GetJobStatusOperation;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.GetOverviewOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.OverviewInfo;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.apache.commons.lang3.ArrayUtils;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpGetCommand;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.util.JsonUtil;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import io.prometheus.client.exporter.common.TextFormat;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_500;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_QPS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_QPS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.CONTEXT_PATH;
import static org.apache.seatunnel.engine.server.rest.RestConstant.FINISHED_JOBS_INFO;
import static org.apache.seatunnel.engine.server.rest.RestConstant.JOB_INFO_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.OVERVIEW;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_JOBS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_JOB_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_THREADS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SYSTEM_MONITORING_INFORMATION;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_WRITE_BYTES;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_WRITE_BYTES_PER_SECONDS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_WRITE_COUNT;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_WRITE_QPS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SOURCE_RECEIVED_BYTES;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SOURCE_RECEIVED_BYTES_PER_SECONDS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SOURCE_RECEIVED_QPS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TELEMETRY_METRICS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TELEMETRY_OPEN_METRICS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.THREAD_DUMP;

public class RestHttpGetCommandProcessor extends HttpCommandProcessor<HttpGetCommand> {

    private final Log4j2HttpGetCommandProcessor original;
    private NodeEngine nodeEngine;

    public RestHttpGetCommandProcessor(TextCommandService textCommandService) {

        this(textCommandService, new Log4j2HttpGetCommandProcessor(textCommandService));
    }

    public RestHttpGetCommandProcessor(
            TextCommandService textCommandService,
            Log4j2HttpGetCommandProcessor log4j2HttpGetCommandProcessor) {
        super(
                textCommandService,
                textCommandService.getNode().getLogger(Log4j2HttpGetCommandProcessor.class));
        this.original = log4j2HttpGetCommandProcessor;
    }

    @Override
    public void handle(HttpGetCommand httpGetCommand) {
        String uri = httpGetCommand.getURI();
        try {
            if (uri.startsWith(CONTEXT_PATH + RUNNING_JOBS_URL)) {
                handleRunningJobsInfo(httpGetCommand);
            } else if (uri.startsWith(CONTEXT_PATH + FINISHED_JOBS_INFO)) {
                handleFinishedJobsInfo(httpGetCommand, uri);
            } else if (uri.startsWith(CONTEXT_PATH + RUNNING_JOB_URL)
                    || uri.startsWith(CONTEXT_PATH + JOB_INFO_URL)) {
                handleJobInfoById(httpGetCommand, uri);
            } else if (uri.startsWith(CONTEXT_PATH + SYSTEM_MONITORING_INFORMATION)) {
                getSystemMonitoringInformation(httpGetCommand);
            } else if (uri.startsWith(CONTEXT_PATH + RUNNING_THREADS)) {
                getRunningThread(httpGetCommand);
            } else if (uri.startsWith(CONTEXT_PATH + OVERVIEW)) {
                overView(httpGetCommand, uri);
            } else if (uri.equals(TELEMETRY_METRICS_URL)) {
                handleMetrics(httpGetCommand, TextFormat.CONTENT_TYPE_004);
            } else if (uri.equals(TELEMETRY_OPEN_METRICS_URL)) {
                handleMetrics(httpGetCommand, TextFormat.CONTENT_TYPE_OPENMETRICS_100);
            } else if (uri.startsWith(CONTEXT_PATH + THREAD_DUMP)) {
                getThreadDump(httpGetCommand);
            } else {
                original.handle(httpGetCommand);
            }
        } catch (IndexOutOfBoundsException e) {
            httpGetCommand.send400();
        } catch (Throwable e) {
            logger.warning("An error occurred while handling request " + httpGetCommand, e);
            prepareResponse(SC_500, httpGetCommand, exceptionResponse(e));
        }

        this.textCommandService.sendResponse(httpGetCommand);
    }

    @Override
    public void handleRejection(HttpGetCommand httpGetCommand) {
        handle(httpGetCommand);
    }

    public void overView(HttpGetCommand command, String uri) {
        uri = StringUtil.stripTrailingSlash(uri);
        String tagStr;
        if (uri.contains("?")) {
            int index = uri.indexOf("?");
            tagStr = uri.substring(index + 1);
        } else {
            tagStr = "";
        }
        Map<String, String> tags =
                Arrays.stream(tagStr.split("&"))
                        .map(variable -> variable.split("=", 2))
                        .filter(pair -> pair.length == 2)
                        .collect(Collectors.toMap(pair -> pair[0], pair -> pair[1]));
        Version version = EnvironmentUtil.getVersion();

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);

        OverviewInfo overviewInfo;

        if (seaTunnelServer == null) {
            overviewInfo =
                    (OverviewInfo)
                            NodeEngineUtil.sendOperationToMasterNode(
                                            getNode().nodeEngine, new GetOverviewOperation(tags))
                                    .join();
            overviewInfo.setProjectVersion(version.getProjectVersion());
            overviewInfo.setGitCommitAbbrev(version.getGitCommitAbbrev());
        } else {

            NodeEngineImpl nodeEngine = this.textCommandService.getNode().getNodeEngine();
            overviewInfo = GetOverviewOperation.getOverviewInfo(seaTunnelServer, nodeEngine, tags);
            overviewInfo.setProjectVersion(version.getProjectVersion());
            overviewInfo.setGitCommitAbbrev(version.getGitCommitAbbrev());
        }

        this.prepareResponse(
                command,
                JsonUtil.toJsonObject(JsonUtils.toMap(JsonUtils.toJsonString(overviewInfo))));
    }

    public void getThreadDump(HttpGetCommand command) {
        Map<Thread, StackTraceElement[]> threadStacks = Thread.getAllStackTraces();
        JsonArray threadInfoList = new JsonArray();
        for (Map.Entry<Thread, StackTraceElement[]> entry : threadStacks.entrySet()) {
            StringBuilder stackTraceBuilder = new StringBuilder();
            for (StackTraceElement element : entry.getValue()) {
                stackTraceBuilder.append(element.toString()).append("\n");
            }
            String stackTrace = stackTraceBuilder.toString().trim();
            JsonObject threadInfo = new JsonObject();
            threadInfo.add("threadName", entry.getKey().getName());
            threadInfo.add("threadId", entry.getKey().getId());
            threadInfo.add("threadState", entry.getKey().getState().name());
            threadInfo.add("stackTrace", stackTrace);
            threadInfoList.add(threadInfo);
        }

        this.prepareResponse(command, threadInfoList);
    }

    private void getSystemMonitoringInformation(HttpGetCommand command) {
        Cluster cluster = textCommandService.getNode().hazelcastInstance.getCluster();
        nodeEngine = textCommandService.getNode().hazelcastInstance.node.nodeEngine;

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
                                        logger.severe("get system monitoring information fail", e);
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
        this.prepareResponse(command, jsonValues);
    }

    private void handleRunningJobsInfo(HttpGetCommand command) {
        IMap<Long, JobInfo> values =
                this.textCommandService
                        .getNode()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_RUNNING_JOB_INFO);
        JsonArray jobs =
                values.entrySet().stream()
                        .map(
                                jobInfoEntry ->
                                        convertToJson(
                                                jobInfoEntry.getValue(), jobInfoEntry.getKey()))
                        .collect(JsonArray::new, JsonArray::add, JsonArray::add);
        this.prepareResponse(command, jobs);
    }

    private void handleFinishedJobsInfo(HttpGetCommand command, String uri) {

        uri = StringUtil.stripTrailingSlash(uri);

        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        String state;
        if (indexEnd == -1) {
            state = "";
        } else {
            state = uri.substring(indexEnd + 1);
        }

        IMap<Long, JobState> finishedJob =
                this.textCommandService
                        .getNode()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_FINISHED_JOB_STATE);

        IMap<Long, JobMetrics> finishedJobMetrics =
                this.textCommandService
                        .getNode()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_FINISHED_JOB_METRICS);

        IMap<Long, JobDAGInfo> finishedJobDAGInfo =
                this.textCommandService
                        .getNode()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO);
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);
        JsonArray jobs =
                finishedJob.values().stream()
                        .filter(
                                jobState -> {
                                    if (state.isEmpty()) {
                                        return true;
                                    }
                                    return jobState.getJobStatus()
                                            .name()
                                            .equals(state.toUpperCase());
                                })
                        .sorted(Comparator.comparing(JobState::getFinishTime))
                        .map(
                                jobState -> {
                                    Long jobId = jobState.getJobId();
                                    String jobMetrics;
                                    if (seaTunnelServer == null) {
                                        jobMetrics =
                                                (String)
                                                        NodeEngineUtil.sendOperationToMasterNode(
                                                                        getNode().nodeEngine,
                                                                        new GetJobMetricsOperation(
                                                                                jobId))
                                                                .join();
                                    } else {
                                        jobMetrics =
                                                seaTunnelServer
                                                        .getCoordinatorService()
                                                        .getJobMetrics(jobId)
                                                        .toJsonString();
                                    }
                                    return getJobInfoJson(
                                            jobState, jobMetrics, finishedJobDAGInfo.get(jobId));
                                })
                        .collect(JsonArray::new, JsonArray::add, JsonArray::add);

        this.prepareResponse(command, jobs);
    }

    private void handleJobInfoById(HttpGetCommand command, String uri) {
        uri = StringUtil.stripTrailingSlash(uri);
        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        String jobId = uri.substring(indexEnd + 1);
        IMap<Object, Object> jobInfoMap =
                this.textCommandService
                        .getNode()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_RUNNING_JOB_INFO);
        JobInfo jobInfo = (JobInfo) jobInfoMap.get(Long.valueOf(jobId));
        IMap<Object, Object> finishedJobStateMap =
                this.textCommandService
                        .getNode()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_FINISHED_JOB_STATE);
        JobState finishedJobState = (JobState) finishedJobStateMap.get(Long.valueOf(jobId));
        if (!jobId.isEmpty() && jobInfo != null) {
            this.prepareResponse(command, convertToJson(jobInfo, Long.parseLong(jobId)));
        } else if (!jobId.isEmpty() && finishedJobState != null) {
            JobMetrics finishedJobMetrics =
                    (JobMetrics)
                            this.textCommandService
                                    .getNode()
                                    .getNodeEngine()
                                    .getHazelcastInstance()
                                    .getMap(Constant.IMAP_FINISHED_JOB_METRICS)
                                    .get(Long.valueOf(jobId));
            JobDAGInfo finishedJobDAGInfo =
                    (JobDAGInfo)
                            this.textCommandService
                                    .getNode()
                                    .getNodeEngine()
                                    .getHazelcastInstance()
                                    .getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO)
                                    .get(Long.valueOf(jobId));
            this.prepareResponse(
                    command,
                    getJobInfoJson(
                            finishedJobState,
                            finishedJobMetrics.toJsonString(),
                            finishedJobDAGInfo));
        } else {
            this.prepareResponse(command, new JsonObject().add(RestConstant.JOB_ID, jobId));
        }
    }

    private void getRunningThread(HttpGetCommand command) {
        this.prepareResponse(
                command,
                Thread.getAllStackTraces().keySet().stream()
                        .sorted(Comparator.comparing(Thread::getName))
                        .map(
                                stackTraceElements -> {
                                    JsonObject jobInfoJson = new JsonObject();
                                    jobInfoJson.add("threadName", stackTraceElements.getName());
                                    jobInfoJson.add(
                                            "classLoader",
                                            String.valueOf(
                                                    stackTraceElements.getContextClassLoader()));
                                    return jobInfoJson;
                                })
                        .collect(JsonArray::new, JsonArray::add, JsonArray::add));
    }

    public static Map<String, Object> getJobMetrics(String jobMetrics) {
        Map<String, Object> metricsMap = new HashMap<>();
        // To add metrics, populate the corresponding array,
        String[] countMetricsNames = {
            SOURCE_RECEIVED_COUNT, SINK_WRITE_COUNT, SOURCE_RECEIVED_BYTES, SINK_WRITE_BYTES
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
            JsonNode jobMetricsStr = new ObjectMapper().readTree(jobMetrics);

            jobMetricsStr
                    .fieldNames()
                    .forEachRemaining(
                            metricName -> {
                                if (metricName.contains("#")) {
                                    String tableName =
                                            TablePath.of(metricName.split("#")[1]).getFullName();
                                    JsonNode metricNode = jobMetricsStr.get(metricName);
                                    processMetric(
                                            metricName, tableName, metricNode, tableMetricsMaps);
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

    public static void processMetric(
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

    public static void aggregateMetrics(
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

    public static void populateMetricsMap(
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

    public static Map<String, Object> aggregateMap(Map<String, JsonNode> inputMap, boolean isRate) {
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

    private void handleMetrics(HttpGetCommand httpGetCommand, String contentType) {
        StringWriter stringWriter = new StringWriter();
        org.apache.seatunnel.engine.server.NodeExtension nodeExtension =
                (org.apache.seatunnel.engine.server.NodeExtension)
                        textCommandService.getNode().getNodeExtension();
        try {
            TextFormat.writeFormat(
                    contentType,
                    stringWriter,
                    nodeExtension.getCollectorRegistry().metricFamilySamples());
            this.prepareResponse(httpGetCommand, stringWriter.toString());
        } catch (IOException e) {
            httpGetCommand.send400();
        } finally {
            try {
                stringWriter.close();
            } catch (IOException e) {
                logger.warning("An error occurred while handling request " + httpGetCommand, e);
                prepareResponse(SC_500, httpGetCommand, exceptionResponse(e));
            }
        }
    }

    private SeaTunnelServer getSeaTunnelServer(boolean shouldBeMaster) {
        Map<String, Object> extensionServices =
                this.textCommandService.getNode().getNodeExtension().createExtensionServices();
        SeaTunnelServer seaTunnelServer =
                (SeaTunnelServer) extensionServices.get(Constant.SEATUNNEL_SERVICE_NAME);
        if (shouldBeMaster && !seaTunnelServer.isMasterNode()) {
            return null;
        }
        return seaTunnelServer;
    }

    private JsonObject convertToJson(JobInfo jobInfo, long jobId) {

        JsonObject jobInfoJson = new JsonObject();
        JobImmutableInformation jobImmutableInformation =
                this.textCommandService
                        .getNode()
                        .getNodeEngine()
                        .getSerializationService()
                        .toObject(
                                this.textCommandService
                                        .getNode()
                                        .getNodeEngine()
                                        .getSerializationService()
                                        .toObject(jobInfo.getJobImmutableInformation()));

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);
        ClassLoaderService classLoaderService =
                seaTunnelServer == null
                        ? getSeaTunnelServer(false).getClassLoaderService()
                        : seaTunnelServer.getClassLoaderService();
        ClassLoader classLoader =
                classLoaderService.getClassLoader(
                        jobId, jobImmutableInformation.getPluginJarsUrls());
        LogicalDag logicalDag =
                CustomClassLoadedObject.deserializeWithCustomClassLoader(
                        this.textCommandService.getNode().getNodeEngine().getSerializationService(),
                        classLoader,
                        jobImmutableInformation.getLogicalDag());
        classLoaderService.releaseClassLoader(jobId, jobImmutableInformation.getPluginJarsUrls());

        String jobMetrics;
        JobStatus jobStatus;
        if (seaTunnelServer == null) {
            jobMetrics =
                    (String)
                            NodeEngineUtil.sendOperationToMasterNode(
                                            getNode().nodeEngine, new GetJobMetricsOperation(jobId))
                                    .join();
            jobStatus =
                    JobStatus.values()[
                            (int)
                                    NodeEngineUtil.sendOperationToMasterNode(
                                                    getNode().nodeEngine,
                                                    new GetJobStatusOperation(jobId))
                                            .join()];
        } else {
            jobMetrics =
                    seaTunnelServer.getCoordinatorService().getJobMetrics(jobId).toJsonString();
            jobStatus = seaTunnelServer.getCoordinatorService().getJobStatus(jobId);
        }

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
                .add(RestConstant.JOB_DAG, logicalDag.getLogicalDagAsJson())
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
                .add(RestConstant.METRICS, toJsonObject(getJobMetrics(jobMetrics)));

        return jobInfoJson;
    }

    private static JsonObject toJsonObject(Map<String, Object> jobMetrics) {
        JsonObject members = new JsonObject();
        jobMetrics.forEach(
                (key, value) -> {
                    if (value instanceof Map) {
                        members.add(key, toJsonObject((Map<String, Object>) value));
                    } else {
                        members.add(key, value.toString());
                    }
                });
        return members;
    }

    public static JsonObject getJobInfoJson(
            JobState jobState, String jobMetrics, JobDAGInfo jobDAGInfo) {
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
                        RestConstant.FINISH_TIME,
                        DateTimeUtils.toString(
                                jobState.getFinishTime(),
                                DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS))
                .add(RestConstant.JOB_DAG, JsonUtils.toJsonString(jobDAGInfo))
                .add(RestConstant.PLUGIN_JARS_URLS, new JsonArray())
                .add(RestConstant.METRICS, toJsonObject(getJobMetrics(jobMetrics)));
    }
}
