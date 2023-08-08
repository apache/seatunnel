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
import org.apache.seatunnel.engine.core.job.AbstractJobEnvironment;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

public class JobExecutionEnvironment extends AbstractJobEnvironment {

    private final String jobFilePath;

    private final SeaTunnelHazelcastClient seaTunnelHazelcastClient;

    private final JobClient jobClient;

    /** If the JobId is not empty, it is used to restore job from savePoint */
    public JobExecutionEnvironment(
            JobConfig jobConfig,
            String jobFilePath,
            SeaTunnelHazelcastClient seaTunnelHazelcastClient,
            boolean isStartWithSavePoint,
            Long jobId) {
        super(jobConfig, isStartWithSavePoint);
        this.jobFilePath = jobFilePath;
        this.seaTunnelHazelcastClient = seaTunnelHazelcastClient;
        this.jobClient = new JobClient(seaTunnelHazelcastClient);
        this.jobConfig.setJobContext(
                new JobContext(isStartWithSavePoint ? jobId : jobClient.getNewJobId()));
    }

    public JobExecutionEnvironment(
            JobConfig jobConfig,
            String jobFilePath,
            SeaTunnelHazelcastClient seaTunnelHazelcastClient) {
        this(jobConfig, jobFilePath, seaTunnelHazelcastClient, false, null);
    }

    /** Search all jars in SEATUNNEL_HOME/plugins */
    @Override
    protected MultipleTableJobConfigParser getJobConfigParser() {
        return new MultipleTableJobConfigParser(
                jobFilePath, idGenerator, jobConfig, commonPluginJars, isStartWithSavePoint);
    }

    public ClientJobProxy execute() throws ExecutionException, InterruptedException {
        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        Long.parseLong(jobConfig.getJobContext().getJobId()),
                        jobConfig.getName(),
                        isStartWithSavePoint,
                        seaTunnelHazelcastClient.getSerializationService().toData(getLogicalDag()),
                        jobConfig,
                        new ArrayList<>(jarUrls));

        return jobClient.createJobProxy(jobImmutableInformation);
    }
}
