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

import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpPostCommand;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.serialization.Data;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_400;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_500;
import static org.apache.seatunnel.engine.server.rest.RestConstant.ENCRYPT_CONFIG;
import static org.apache.seatunnel.engine.server.rest.RestConstant.STOP_JOB_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SUBMIT_JOB_URL;

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
            if (uri.startsWith(SUBMIT_JOB_URL)) {
                handleSubmitJob(httpPostCommand, uri);
            } else if (uri.startsWith(STOP_JOB_URL)) {
                handleStopJob(httpPostCommand, uri);
            } else if (uri.startsWith(ENCRYPT_CONFIG)) {
                handleEncrypt(httpPostCommand);
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

    private void handleSubmitJob(HttpPostCommand httpPostCommand, String uri)
            throws IllegalArgumentException {
        Map<String, String> requestParams = new HashMap<>();
        RestUtil.buildRequestParams(requestParams, uri);
        Config config = RestUtil.buildConfig(requestHandle(httpPostCommand), false);
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
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer();
        RestJobExecutionEnvironment restJobExecutionEnvironment =
                new RestJobExecutionEnvironment(
                        seaTunnelServer,
                        jobConfig,
                        config,
                        textCommandService.getNode(),
                        startWithSavePoint,
                        finalJobId);
        JobImmutableInformation jobImmutableInformation = restJobExecutionEnvironment.build();
        long jobId = jobImmutableInformation.getJobId();
        if (!seaTunnelServer.isMasterNode()) {

            NodeEngineUtil.sendOperationToMasterNode(
                            getNode().nodeEngine,
                            new SubmitJobOperation(
                                    jobId,
                                    getNode().nodeEngine.toData(jobImmutableInformation),
                                    jobImmutableInformation.isStartWithSavePoint()))
                    .join();

        } else {
            submitJob(seaTunnelServer, jobImmutableInformation, jobConfig);
        }

        this.prepareResponse(
                httpPostCommand,
                new JsonObject()
                        .add(RestConstant.JOB_ID, String.valueOf(jobId))
                        .add(RestConstant.JOB_NAME, jobConfig.getName()));
    }

    private void handleStopJob(HttpPostCommand httpPostCommand, String uri) {
        Map<String, Object> map = JsonUtils.toMap(requestHandle(httpPostCommand));
        boolean isStopWithSavePoint = false;
        if (map.get(RestConstant.JOB_ID) == null) {
            throw new IllegalArgumentException("jobId cannot be empty.");
        }
        long jobId = Long.parseLong(map.get(RestConstant.JOB_ID).toString());
        if (map.get(RestConstant.IS_STOP_WITH_SAVE_POINT) != null) {
            isStopWithSavePoint =
                    Boolean.parseBoolean(map.get(RestConstant.IS_STOP_WITH_SAVE_POINT).toString());
        }

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer();
        if (!seaTunnelServer.isMasterNode()) {
            if (isStopWithSavePoint) {
                NodeEngineUtil.sendOperationToMasterNode(
                                getNode().nodeEngine, new SavePointJobOperation(jobId))
                        .join();
            } else {
                NodeEngineUtil.sendOperationToMasterNode(
                                getNode().nodeEngine, new CancelJobOperation(jobId))
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

        this.prepareResponse(
                httpPostCommand,
                new JsonObject().add(RestConstant.JOB_ID, map.get(RestConstant.JOB_ID).toString()));
    }

    private void handleEncrypt(HttpPostCommand httpPostCommand) {
        Config config = RestUtil.buildConfig(requestHandle(httpPostCommand), true);
        Config encryptConfig = ConfigShadeUtils.encryptConfig(config);
        String encryptString =
                encryptConfig.root().render(ConfigRenderOptions.concise().setJson(true));
        JsonObject jsonObject = Json.parse(encryptString).asObject();
        this.prepareResponse(httpPostCommand, jsonObject);
    }

    @Override
    public void handleRejection(HttpPostCommand httpPostCommand) {
        handle(httpPostCommand);
    }

    private JsonNode requestHandle(HttpPostCommand httpPostCommand) {
        byte[] requestBody = httpPostCommand.getData();
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

    private void submitJob(
            SeaTunnelServer seaTunnelServer,
            JobImmutableInformation jobImmutableInformation,
            JobConfig jobConfig) {
        CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();
        Data data =
                textCommandService
                        .getNode()
                        .nodeEngine
                        .getSerializationService()
                        .toData(jobImmutableInformation);
        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                coordinatorService.submitJob(
                        Long.parseLong(jobConfig.getJobContext().getJobId()),
                        data,
                        jobImmutableInformation.isStartWithSavePoint());
        voidPassiveCompletableFuture.join();
    }
}
