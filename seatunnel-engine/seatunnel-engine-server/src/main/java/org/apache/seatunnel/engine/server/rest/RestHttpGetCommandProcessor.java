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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.log.Log4j2HttpGetCommandProcessor;
import org.apache.seatunnel.engine.server.operation.GetClusterHealthMetricsOperation;
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

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_500;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_JOBS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_JOB_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SYSTEM_MONITORING_INFORMATION;

public class RestHttpGetCommandProcessor extends HttpCommandProcessor<HttpGetCommand> {

    private final Log4j2HttpGetCommandProcessor original;

    private static final String SOURCE_RECEIVED_COUNT = "SourceReceivedCount";

    private static final String SINK_WRITE_COUNT = "SinkWriteCount";

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
            } else if (uri.startsWith(RUNNING_JOB_URL)) {
                handleJobInfoById(httpGetCommand, uri);
            } else if (uri.startsWith(SYSTEM_MONITORING_INFORMATION)) {
                getSystemMonitoringInformation(httpGetCommand);
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
                                    assert input != null;
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

    private void handleJobInfoById(HttpGetCommand command, String uri) {
        uri = StringUtil.stripTrailingSlash(uri);
        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        String jobId = uri.substring(indexEnd + 1);

        JobInfo jobInfo =
                (JobInfo)
                        this.textCommandService
                                .getNode()
                                .getNodeEngine()
                                .getHazelcastInstance()
                                .getMap(Constant.IMAP_RUNNING_JOB_INFO)
                                .get(Long.valueOf(jobId));

        if (!"".equals(jobId) && jobInfo != null) {
            this.prepareResponse(command, convertToJson(jobInfo, Long.parseLong(jobId)));
        } else {
            this.prepareResponse(command, new JsonObject());
        }
    }

    private Map<String, Long> getJobMetrics(String jobMetrics) {
        Map<String, Long> metricsMap = new HashMap<>();
        long sourceReadCount = 0L;
        long sinkWriteCount = 0L;
        try {
            JsonNode jobMetricsStr = new ObjectMapper().readTree(jobMetrics);
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
        metricsMap.put("sourceReceivedCount", sourceReadCount);
        metricsMap.put("sinkWriteCount", sinkWriteCount);

        return metricsMap;
    }

    private SeaTunnelServer getSeaTunnelServer() {
        Map<String, Object> extensionServices =
                this.textCommandService.getNode().getNodeExtension().createExtensionServices();
        return (SeaTunnelServer) extensionServices.get(Constant.SEATUNNEL_SERVICE_NAME);
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

        ClassLoader classLoader =
                new SeaTunnelChildFirstClassLoader(jobImmutableInformation.getPluginJarsUrls());
        LogicalDag logicalDag =
                CustomClassLoadedObject.deserializeWithCustomClassLoader(
                        this.textCommandService.getNode().getNodeEngine().getSerializationService(),
                        classLoader,
                        jobImmutableInformation.getLogicalDag());

        String jobMetrics =
                getSeaTunnelServer().getCoordinatorService().getJobMetrics(jobId).toJsonString();
        JobStatus jobStatus = getSeaTunnelServer().getCoordinatorService().getJobStatus(jobId);

        jobInfoJson
                .add("jobId", jobId)
                .add("jobName", logicalDag.getJobConfig().getName())
                .add("jobStatus", jobStatus.toString())
                .add("envOptions", JsonUtil.toJsonObject(logicalDag.getJobConfig().getEnvOptions()))
                .add(
                        "createTime",
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                                .format(new Date(jobImmutableInformation.getCreateTime())))
                .add("jobDag", logicalDag.getLogicalDagAsJson())
                .add(
                        "pluginJarsUrls",
                        (JsonValue)
                                jobImmutableInformation.getPluginJarsUrls().stream()
                                        .map(
                                                url -> {
                                                    JsonObject jarUrl = new JsonObject();
                                                    jarUrl.add("jarPath", url.toString());
                                                    return jarUrl;
                                                })
                                        .collect(JsonArray::new, JsonArray::add, JsonArray::add))
                .add("isStartWithSavePoint", jobImmutableInformation.isStartWithSavePoint())
                .add("metrics", JsonUtil.toJsonObject(getJobMetrics(jobMetrics)));

        return jobInfoJson;
    }
}
