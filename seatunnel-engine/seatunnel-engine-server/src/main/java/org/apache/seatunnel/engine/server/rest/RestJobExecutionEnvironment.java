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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.AbstractJobEnvironment;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.google.common.annotations.VisibleForTesting;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class RestJobExecutionEnvironment extends AbstractJobEnvironment {
    private final Config seaTunnelJobConfig;

    private final NodeEngineImpl nodeEngine;

    private final Long jobId;

    private final SeaTunnelServer seaTunnelServer;

    public RestJobExecutionEnvironment(
            SeaTunnelServer seaTunnelServer,
            JobConfig jobConfig,
            Config seaTunnelJobConfig,
            Node node,
            boolean isStartWithSavePoint,
            Long jobId) {
        super(jobConfig, isStartWithSavePoint);
        this.seaTunnelServer = seaTunnelServer;
        this.seaTunnelJobConfig = seaTunnelJobConfig;
        this.nodeEngine = node.getNodeEngine();
        this.jobConfig.setJobContext(
                new JobContext(
                        Objects.nonNull(jobId)
                                ? jobId
                                : nodeEngine
                                        .getHazelcastInstance()
                                        .getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME)
                                        .newId()));
        this.jobId = Long.valueOf(this.jobConfig.getJobContext().getJobId());
    }

    public Long getJobId() {
        return jobId;
    }

    @VisibleForTesting
    @Override
    public LogicalDag getLogicalDag() {
        ImmutablePair<List<Action>, Set<URL>> immutablePair =
                getJobConfigParser().parse(seaTunnelServer.getClassLoaderService());
        actions.addAll(immutablePair.getLeft());
        jarUrls.addAll(commonPluginJars);
        jarUrls.addAll(immutablePair.getRight());
        actions.forEach(
                action -> {
                    addCommonPluginJarsToAction(
                            action, new HashSet<>(commonPluginJars), Collections.emptySet());
                });
        return getLogicalDagGenerator().generate();
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
                new ArrayList<>(jarUrls),
                new ArrayList<>(connectorJarIdentifiers));
    }
}
