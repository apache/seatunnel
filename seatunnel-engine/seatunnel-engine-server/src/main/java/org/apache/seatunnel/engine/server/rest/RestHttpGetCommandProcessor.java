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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.log.Log4j2HttpGetCommandProcessor;

import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpGetCommand;
import com.hazelcast.internal.ascii.rest.HttpGetCommandProcessor;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.util.JsonUtil;
import com.hazelcast.map.IMap;

import java.text.SimpleDateFormat;
import java.util.Date;

public class RestHttpGetCommandProcessor extends HttpCommandProcessor<HttpGetCommand> {

    private final HttpGetCommandProcessor original;

    public RestHttpGetCommandProcessor(TextCommandService textCommandService) {
        this(textCommandService, new HttpGetCommandProcessor(textCommandService));
    }

    public RestHttpGetCommandProcessor(TextCommandService textCommandService,
                                       HttpGetCommandProcessor httpGetCommandProcessor) {
        super(textCommandService, textCommandService.getNode().getLogger(Log4j2HttpGetCommandProcessor.class));
        this.original = httpGetCommandProcessor;
    }

    @Override
    public void handle(HttpGetCommand httpGetCommand) {
        String uri = httpGetCommand.getURI();
        if (uri.startsWith("/hazelcast/rest/maps/running-jobs")) {
            handleRunningJobsInfo(httpGetCommand);
        } else {
            original.handle(httpGetCommand);
        }

        this.textCommandService.sendResponse(httpGetCommand);

    }

    @Override
    public void handleRejection(HttpGetCommand httpGetCommand) {
        handle(httpGetCommand);
    }

    private void handleRunningJobsInfo(HttpGetCommand command) {
        IMap<Long, JobInfo> values = this.textCommandService.getNode().getNodeEngine().getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        JsonArray jobs = values.entrySet().stream()
            .map(jobInfoEntry -> {
                JsonObject jobInfo = new JsonObject();
                JobImmutableInformation jobImmutableInformation = this.textCommandService.getNode().getNodeEngine().getSerializationService()
                    .toObject(this.textCommandService.getNode().getNodeEngine().getSerializationService()
                        .toObject(jobInfoEntry.getValue().getJobImmutableInformation()));
                LogicalDag logicalDag = this.textCommandService.getNode().getNodeEngine().getSerializationService()
                    .toObject(jobImmutableInformation.getLogicalDag());

                return jobInfo
                    .add("jobId", jobInfoEntry.getKey())
                    .add("jobName", logicalDag.getJobConfig().getName())
                    .add("envOptions", JsonUtil.toJsonObject(logicalDag.getJobConfig().getEnvOptions()))
                    .add("createTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(jobImmutableInformation.getCreateTime())))
                    .add("jobDag", logicalDag.getLogicalDagAsJson())
                    .add("pluginJarsUrls", (JsonValue) jobImmutableInformation.getPluginJarsUrls().stream().map(url -> {
                        JsonObject jarUrl = new JsonObject();
                        jarUrl.add("jarPath", url.toString());
                        return jarUrl;
                    }).collect(JsonArray::new, JsonArray::add, JsonArray::add))
                    .add("isStartWithSavePoint", jobImmutableInformation.isStartWithSavePoint());
            }).collect(JsonArray::new, JsonArray::add, JsonArray::add);
        this.prepareResponse(command, jobs);
    }
}
