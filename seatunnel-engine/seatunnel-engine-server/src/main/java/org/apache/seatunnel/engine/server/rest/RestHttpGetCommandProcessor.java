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

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_500;
import static org.apache.seatunnel.engine.server.rest.RestConstant.FINISHED_JOBS_INFO;
import static org.apache.seatunnel.engine.server.rest.RestConstant.JOB_INFO_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.OVERVIEW;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_JOBS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_JOB_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_THREADS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SYSTEM_MONITORING_INFORMATION;

public class RestHttpGetCommandProcessor extends HttpCommandProcessor<HttpGetCommand> {

    private static final String SOURCE_RECEIVED_COUNT = "SourceReceivedCount";
    private static final String TABLE_SOURCE_RECEIVED_COUNT = "TableSourceReceivedCount";
    private static final String SINK_WRITE_COUNT = "SinkWriteCount";
    private static final String TABLE_SINK_WRITE_COUNT = "TableSinkWriteCount";
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
            if (uri.startsWith(RUNNING_JOBS_URL)) {
                handleRunningJobsInfo(httpGetCommand);
            } else if (uri.startsWith(FINISHED_JOBS_INFO)) {
                handleFinishedJobsInfo(httpGetCommand, uri);
            } else if (uri.startsWith(RUNNING_JOB_URL) || uri.startsWith(JOB_INFO_URL)) {
                handleJobInfoById(httpGetCommand, uri);
            } else if (uri.startsWith(SYSTEM_MONITORING_INFORMATION)) {
                getSystemMonitoringInformation(httpGetCommand);
            } else if (uri.startsWith(RUNNING_THREADS)) {
                getRunningThread(httpGetCommand);
            } else if (uri.startsWith(OVERVIEW)) {
                overView(httpGetCommand, uri);
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

    private Map<String, Object> getJobMetrics(String jobMetrics) {
        Map<String, Object> metricsMap = new HashMap<>();
        long sourceReadCount = 0L;
        long sinkWriteCount = 0L;
        Map<String, JsonNode> tableSourceReceivedCountMap = new HashMap<>();
        Map<String, JsonNode> tableSinkWriteCountMap = new HashMap<>();
        try {
            JsonNode jobMetricsStr = new ObjectMapper().readTree(jobMetrics);
            StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(jobMetricsStr.fieldNames(), 0),
                            false)
                    .filter(metricName -> metricName.contains("#"))
                    .forEach(
                            metricName -> {
                                String tableName =
                                        TablePath.of(metricName.split("#")[1]).getFullName();
                                if (metricName.startsWith(SOURCE_RECEIVED_COUNT)) {
                                    tableSourceReceivedCountMap.put(
                                            tableName, jobMetricsStr.get(metricName));
                                }
                                if (metricName.startsWith(SOURCE_RECEIVED_COUNT)) {
                                    tableSinkWriteCountMap.put(
                                            tableName, jobMetricsStr.get(metricName));
                                }
                            });
            JsonNode sourceReceivedCountJson = jobMetricsStr.get(SOURCE_RECEIVED_COUNT);
            JsonNode sinkWriteCountJson = jobMetricsStr.get(SINK_WRITE_COUNT);
            for (int i = 0; i < jobMetricsStr.get(SOURCE_RECEIVED_COUNT).size(); i++) {
                JsonNode sourceReader = sourceReceivedCountJson.get(i);
                JsonNode sinkWriter = sinkWriteCountJson.get(i);
                sourceReadCount += sourceReader.get("value").asLong();
                sinkWriteCount += sinkWriter.get("value").asLong();
            }
        } catch (JsonProcessingException | NullPointerException e) {
            return metricsMap;
        }

        Map<String, Long> tableSourceReceivedCount =
                tableSourceReceivedCountMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry ->
                                                StreamSupport.stream(
                                                                entry.getValue().spliterator(),
                                                                false)
                                                        .mapToLong(
                                                                node -> node.get("value").asLong())
                                                        .sum()));
        Map<String, Long> tableSinkWriteCount =
                tableSinkWriteCountMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry ->
                                                StreamSupport.stream(
                                                                entry.getValue().spliterator(),
                                                                false)
                                                        .mapToLong(
                                                                node -> node.get("value").asLong())
                                                        .sum()));

        metricsMap.put(SOURCE_RECEIVED_COUNT, sourceReadCount);
        metricsMap.put(SINK_WRITE_COUNT, sinkWriteCount);
        metricsMap.put(TABLE_SOURCE_RECEIVED_COUNT, tableSourceReceivedCount);
        metricsMap.put(TABLE_SINK_WRITE_COUNT, tableSinkWriteCount);
        return metricsMap;
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

    private JsonObject toJsonObject(Map<String, Object> jobMetrics) {
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

    private JsonObject getJobInfoJson(JobState jobState, String jobMetrics, JobDAGInfo jobDAGInfo) {
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
