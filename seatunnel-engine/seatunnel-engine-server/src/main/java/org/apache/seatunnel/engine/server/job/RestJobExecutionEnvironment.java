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

package org.apache.seatunnel.engine.server.job;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.core.job.AbstractJobEnvironment;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.ArrayList;

public class RestJobExecutionEnvironment extends AbstractJobEnvironment {
    private final Config seaTunnelJobConfig;

    private final NodeEngineImpl nodeEngine;

    private final Long jobId;

    public RestJobExecutionEnvironment(
            JobConfig jobConfig,
            Config seaTunnelJobConfig,
            Node node,
            boolean isStartWithSavePoint,
            Long jobId) {
        super(jobConfig, isStartWithSavePoint);
        this.seaTunnelJobConfig = seaTunnelJobConfig;
        this.nodeEngine = node.getNodeEngine();
        this.jobConfig.setJobContext(
                new JobContext(
                        isStartWithSavePoint
                                ? jobId
                                : nodeEngine
                                        .getHazelcastInstance()
                                        .getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME)
                                        .newId()));
        this.jobId = Long.valueOf(jobConfig.getJobContext().getJobId());
    }

    public Long getJobId() {
        return jobId;
    }

    @Override
    protected MultipleTableJobConfigParser getJobConfigParser() {
        return new MultipleTableJobConfigParser(
                seaTunnelJobConfig, idGenerator, jobConfig, commonPluginJars, isStartWithSavePoint);
    }

    public JobImmutableInformation build() {
        return new JobImmutableInformation(
                Long.parseLong(jobConfig.getJobContext().getJobId()),
                jobConfig.getName(),
                isStartWithSavePoint,
                nodeEngine.getSerializationService().toData(getLogicalDag()),
                jobConfig,
                new ArrayList<>(jarUrls));
    }
}
