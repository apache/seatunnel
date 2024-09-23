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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.log.Log4j2HttpPostCommandProcessor;
import org.apache.seatunnel.engine.server.operation.CancelJobOperation;
import org.apache.seatunnel.engine.server.operation.SavePointJobOperation;
import org.apache.seatunnel.engine.server.operation.SubmitJobOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;
import org.apache.seatunnel.engine.server.utils.RestUtil;

import org.apache.commons.lang3.StringUtils;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpPostCommand;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_400;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_500;
import static org.apache.seatunnel.engine.server.rest.RestConstant.CONTEXT_PATH;
import static org.apache.seatunnel.engine.server.rest.RestConstant.ENCRYPT_CONFIG;
import static org.apache.seatunnel.engine.server.rest.RestConstant.STOP_JOBS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.STOP_JOB_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SUBMIT_JOBS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SUBMIT_JOB_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.UPDATE_TAGS_URL;

@Slf4j
public class RestHttpPostCommandProcessor extends HttpCommandProcessor<HttpPostCommand> {

    private final Log4j2HttpPostCommandProcessor original;

    public RestHttpPostCommandProcessor(TextCommandService textCommandService) {
        this(textCommandService, new Log4j2HttpPostCommandProcessor(textCommandService));
    }

    protected RestHttpPostCommandProcessor(
            TextCommandService textCommandService,
            Log4j2HttpPostCommandProcessor log4j2HttpPostCommandProcessor) {
        super(
                textCommandService,
                textCommandService.getNode().getLogger(Log4j2HttpPostCommandProcessor.class));
        this.original = log4j2HttpPostCommandProcessor;
    }

    @Override
    public void handle(HttpPostCommand httpPostCommand) {
        String uri = httpPostCommand.getURI();
        try {
            if (uri.startsWith(CONTEXT_PATH + SUBMIT_JOBS_URL)) {
                handleSubmitJobs(httpPostCommand);
            } else if (uri.startsWith(CONTEXT_PATH + SUBMIT_JOB_URL)) {
                handleSubmitJob(httpPostCommand, uri);
            } else if (uri.startsWith(CONTEXT_PATH + STOP_JOBS_URL)) {
                handleStopJobs(httpPostCommand);
            } else if (uri.startsWith(CONTEXT_PATH + STOP_JOB_URL)) {
                handleStopJob(httpPostCommand);
            } else if (uri.startsWith(CONTEXT_PATH + ENCRYPT_CONFIG)) {
                handleEncrypt(httpPostCommand);
            } else if (uri.startsWith(CONTEXT_PATH + UPDATE_TAGS_URL)) {
                handleUpdateTags(httpPostCommand);
            } else {
                original.handle(httpPostCommand);
            }
        } catch (IllegalArgumentException e) {
            prepareResponse(SC_400, httpPostCommand, exceptionResponse(e));
        } catch (Throwable e) {
            logger.warning("An error occurred while handling request " + httpPostCommand, e);
            prepareResponse(SC_500, httpPostCommand, exceptionResponse(e));
        }
        this.textCommandService.sendResponse(httpPostCommand);
    }

    private SeaTunnelServer getSeaTunnelServer() {
        Map<String, Object> extensionServices =
                this.textCommandService.getNode().getNodeExtension().createExtensionServices();
        return (SeaTunnelServer) extensionServices.get(Constant.SEATUNNEL_SERVICE_NAME);
    }

    private void handleSubmitJobs(HttpPostCommand httpPostCommand) throws IllegalArgumentException {
        List<Tuple2<Map<String, String>, Config>> configTuples =
                RestUtil.buildConfigList(requestHandle(httpPostCommand.getData()), false);

        JsonArray jsonArray =
                configTuples.stream()
                        .map(
                                tuple -> {
                                    String urlParams = mapToUrlParams(tuple._1);
                                    Map<String, String> requestParams = new HashMap<>();
                                    RestUtil.buildRequestParams(requestParams, urlParams);
                                    SeaTunnelServer seaTunnelServer = getSeaTunnelServer();
                                    Node node = textCommandService.getNode();
                                    return submitJobInternal(
                                            tuple._2, requestParams, seaTunnelServer, node);
                                })
                        .collect(JsonArray::new, JsonArray::add, JsonArray::add);

        prepareResponse(httpPostCommand, jsonArray);
    }

    public static String mapToUrlParams(Map<String, String> params) {
        return params.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&", "?", ""));
    }

    private void handleSubmitJob(HttpPostCommand httpPostCommand, String uri)
            throws IllegalArgumentException {
        Map<String, String> requestParams = new HashMap<>();
        RestUtil.buildRequestParams(requestParams, uri);
        Config config = RestUtil.buildConfig(requestHandle(httpPostCommand.getData()), false);
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer();
        Node node = textCommandService.getNode();
        JsonObject jsonObject = submitJobInternal(config, requestParams, seaTunnelServer, node);
        this.prepareResponse(httpPostCommand, jsonObject);
    }

    public static JsonObject submitJobInternal(
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

    private void handleStopJobs(HttpPostCommand command) {
        List<Map> jobList =
                JsonUtils.toList(requestHandle(command.getData()).toString(), Map.class);
        JsonArray jsonResponse = new JsonArray();
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer();
        Node node = textCommandService.getNode();
        jobList.forEach(
                job -> {
                    handleStopJob(job, seaTunnelServer, node);
                    jsonResponse.add(
                            new JsonObject()
                                    .add(RestConstant.JOB_ID, (Long) job.get(RestConstant.JOB_ID)));
                });

        this.prepareResponse(command, jsonResponse);
    }

    private void handleStopJob(HttpPostCommand httpPostCommand) {
        Map<String, Object> map = JsonUtils.toMap(requestHandle(httpPostCommand.getData()));
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer();
        Node node = textCommandService.getNode();
        handleStopJob(map, seaTunnelServer, node);
        this.prepareResponse(
                httpPostCommand,
                new JsonObject().add(RestConstant.JOB_ID, map.get(RestConstant.JOB_ID).toString()));
    }

    public static void handleStopJob(
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

        if (!seaTunnelServer.isMasterNode()) {
            if (isStopWithSavePoint) {
                NodeEngineUtil.sendOperationToMasterNode(
                                node.nodeEngine, new SavePointJobOperation(jobId))
                        .join();
            } else {
                NodeEngineUtil.sendOperationToMasterNode(
                                node.nodeEngine, new CancelJobOperation(jobId))
                        .join();
            }

        } else {
            CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();

            if (isStopWithSavePoint) {
                coordinatorService.savePoint(jobId);
            } else {
                coordinatorService.cancelJob(jobId);
            }
        }
        log.info("Stop job with jobId: " + jobId);
    }

    private void handleEncrypt(HttpPostCommand httpPostCommand) {
        Config config = RestUtil.buildConfig(requestHandle(httpPostCommand.getData()), true);
        Config encryptConfig = ConfigShadeUtils.encryptConfig(config);
        String encryptString =
                encryptConfig.root().render(ConfigRenderOptions.concise().setJson(true));
        JsonObject jsonObject = Json.parse(encryptString).asObject();
        this.prepareResponse(httpPostCommand, jsonObject);
    }

    private void handleUpdateTags(HttpPostCommand httpPostCommand) {
        Map<String, Object> params = JsonUtils.toMap(requestHandle(httpPostCommand.getData()));
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer();

        NodeEngineImpl nodeEngine = seaTunnelServer.getNodeEngine();
        MemberImpl localMember = nodeEngine.getLocalMember();

        Map<String, String> tags =
                params.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        value ->
                                                value.getValue() != null
                                                        ? value.getValue().toString()
                                                        : ""));
        localMember.updateAttribute(tags);
        this.prepareResponse(
                httpPostCommand,
                new JsonObject()
                        .add("status", ResponseType.SUCCESS.toString())
                        .add("message", "update node tags done."));
    }

    @Override
    public void handleRejection(HttpPostCommand httpPostCommand) {
        handle(httpPostCommand);
    }

    public static JsonNode requestHandle(byte[] requestBody) {
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

    private static void submitJob(
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
}
