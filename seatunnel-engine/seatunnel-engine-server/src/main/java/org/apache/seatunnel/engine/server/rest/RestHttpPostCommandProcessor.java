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

import org.apache.seatunnel.engine.server.log.Log4j2HttpPostCommandProcessor;
import org.apache.seatunnel.engine.server.rest.service.EncryptConfigService;
import org.apache.seatunnel.engine.server.rest.service.JobInfoService;
import org.apache.seatunnel.engine.server.rest.service.UpdateTagsService;
import org.apache.seatunnel.engine.server.utils.RestUtil;

import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpPostCommand;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_400;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_500;
import static org.apache.seatunnel.engine.server.rest.RestConstant.CONTEXT_PATH;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_ENCRYPT_CONFIG;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_STOP_JOB;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_STOP_JOBS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_SUBMIT_JOB;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_SUBMIT_JOBS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_UPDATE_TAGS;

@Slf4j
public class RestHttpPostCommandProcessor extends HttpCommandProcessor<HttpPostCommand> {

    private final Log4j2HttpPostCommandProcessor original;
    private JobInfoService jobInfoService;
    private EncryptConfigService encryptConfigService;
    private UpdateTagsService updateTagsService;

    public RestHttpPostCommandProcessor(TextCommandService textCommandService) {
        this(textCommandService, new Log4j2HttpPostCommandProcessor(textCommandService));
        this.jobInfoService = new JobInfoService(this.textCommandService.getNode().getNodeEngine());
        this.encryptConfigService =
                new EncryptConfigService(this.textCommandService.getNode().getNodeEngine());
        this.updateTagsService =
                new UpdateTagsService(this.textCommandService.getNode().getNodeEngine());
    }

    protected RestHttpPostCommandProcessor(
            TextCommandService textCommandService,
            Log4j2HttpPostCommandProcessor log4j2HttpPostCommandProcessor) {
        super(
                textCommandService,
                textCommandService.getNode().getLogger(Log4j2HttpPostCommandProcessor.class));
        this.original = log4j2HttpPostCommandProcessor;
        this.jobInfoService = new JobInfoService(this.textCommandService.getNode().getNodeEngine());
        this.encryptConfigService =
                new EncryptConfigService(this.textCommandService.getNode().getNodeEngine());
        this.updateTagsService =
                new UpdateTagsService(this.textCommandService.getNode().getNodeEngine());
    }

    @Override
    public void handle(HttpPostCommand httpPostCommand) {
        String uri = httpPostCommand.getURI();
        try {
            if (uri.startsWith(CONTEXT_PATH + REST_URL_SUBMIT_JOBS)) {
                handleSubmitJobs(httpPostCommand);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_SUBMIT_JOB)) {
                handleSubmitJob(httpPostCommand, uri);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_STOP_JOBS)) {
                handleStopJobs(httpPostCommand);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_STOP_JOB)) {
                handleStopJob(httpPostCommand);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_ENCRYPT_CONFIG)) {
                handleEncrypt(httpPostCommand);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_UPDATE_TAGS)) {
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

    private void handleSubmitJobs(HttpPostCommand httpPostCommand) throws IllegalArgumentException {

        prepareResponse(httpPostCommand, jobInfoService.submitJobs(httpPostCommand.getData()));
    }

    private void handleSubmitJob(HttpPostCommand httpPostCommand, String uri)
            throws IllegalArgumentException {
        Map<String, String> requestParams = new HashMap<>();
        RestUtil.buildRequestParams(requestParams, uri);
        this.prepareResponse(
                httpPostCommand,
                jobInfoService.submitJob(requestParams, httpPostCommand.getData()));
    }

    private void handleStopJobs(HttpPostCommand command) {

        this.prepareResponse(command, jobInfoService.stopJobs(command.getData()));
    }

    private void handleStopJob(HttpPostCommand httpPostCommand) {
        this.prepareResponse(httpPostCommand, jobInfoService.stopJob(httpPostCommand.getData()));
    }

    private void handleEncrypt(HttpPostCommand httpPostCommand) {
        this.prepareResponse(
                httpPostCommand, encryptConfigService.encryptConfig(httpPostCommand.getData()));
    }

    private void handleUpdateTags(HttpPostCommand httpPostCommand) {
        this.prepareResponse(
                httpPostCommand, updateTagsService.updateTags(httpPostCommand.getData()));
    }

    @Override
    public void handleRejection(HttpPostCommand httpPostCommand) {
        handle(httpPostCommand);
    }
}
