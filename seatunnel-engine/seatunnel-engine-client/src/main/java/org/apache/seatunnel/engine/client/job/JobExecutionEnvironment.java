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

package org.apache.seatunnel.engine.client.job;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.engine.client.SeaTunnelHazelcastClient;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDagGenerator;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.parse.JobConfigParser;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class JobExecutionEnvironment {

    private final JobConfig jobConfig;

    private final int maxParallelism = 1;

    private final List<Action> actions = new ArrayList<>();

    private final List<URL> jarUrls = new ArrayList<>();

    private final String jobFilePath;

    private final IdGenerator idGenerator;

    private final SeaTunnelHazelcastClient seaTunnelHazelcastClient;

    private final JobClient jobClient;

    public JobExecutionEnvironment(JobConfig jobConfig, String jobFilePath,
                                   SeaTunnelHazelcastClient seaTunnelHazelcastClient) {
        this.jobConfig = jobConfig;
        this.jobFilePath = jobFilePath;
        this.idGenerator = new IdGenerator();
        this.seaTunnelHazelcastClient = seaTunnelHazelcastClient;
        this.jobClient = new JobClient(seaTunnelHazelcastClient);
        this.jobConfig.setJobContext(new JobContext(jobClient.getNewJobId()));
    }

    private JobConfigParser getJobConfigParser() {
        return new JobConfigParser(jobFilePath, idGenerator, jobConfig);
    }

    public void addAction(List<Action> actions) {
        this.actions.addAll(actions);
    }

    private LogicalDagGenerator getLogicalDagGenerator() {
        return new LogicalDagGenerator(actions, jobConfig, idGenerator);
    }

    public List<Action> getActions() {
        return actions;
    }

    public ClientJobProxy execute() throws ExecutionException, InterruptedException {
        JobImmutableInformation jobImmutableInformation = new JobImmutableInformation(
            Long.parseLong(jobConfig.getJobContext().getJobId()),
            seaTunnelHazelcastClient.getSerializationService().toData(getLogicalDag()),
            jobConfig,
            jarUrls);

        return jobClient.createJobProxy(jobImmutableInformation);
    }

    public LogicalDag getLogicalDag() {
        ImmutablePair<List<Action>, Set<URL>> immutablePair = getJobConfigParser().parse();
        actions.addAll(immutablePair.getLeft());
        jarUrls.addAll(immutablePair.getRight());
        return getLogicalDagGenerator().generate();
    }
}

