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

import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_JSON;
import static org.apache.seatunnel.engine.common.Constant.IMAP_RUNNING_JOB_INFO;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.log.Log4j2HttpGetCommandProcessor;

import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpGetCommand;
import com.hazelcast.internal.ascii.rest.HttpGetCommandProcessor;
import com.hazelcast.internal.ascii.rest.RestValue;
import com.hazelcast.map.IMap;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

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
            handleJobs(httpGetCommand);
        } else {
            original.handle(httpGetCommand);
        }

        this.textCommandService.sendResponse(httpGetCommand);

    }

    @Override
    public void handleRejection(HttpGetCommand httpGetCommand) {
        handle(httpGetCommand);
    }

    private void handleJobs(HttpGetCommand command) {
        IMap<Long, JobInfo> values = this.textCommandService.getNode().getNodeEngine().getHazelcastInstance().getMap(IMAP_RUNNING_JOB_INFO);
        List<String> jobs = values.entrySet().stream()
            .map(jobInfoEntry -> ((LogicalDag) this.textCommandService.getNode().getNodeEngine().getSerializationService()
                .toObject(((JobImmutableInformation) this.textCommandService.getNode().getNodeEngine().getSerializationService()
                    .toObject(jobInfoEntry.getValue().getJobImmutableInformation()))
                    .getLogicalDag())).getLogicalDagAsJson()
                .add("jobId", jobInfoEntry.getKey())
                .add("InitializationTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(jobInfoEntry.getValue().getInitializationTimestamp())))
                .toString())
            .collect(Collectors.toList());

        this.prepareResponse(command, new RestValue(JsonUtils.toJsonString(jobs).getBytes(StandardCharsets.UTF_8), CONTENT_TYPE_JSON));
    }
}
