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
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelHazelcastClient;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDagGenerator;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.parse.JobConfigParser;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class JobExecutionEnvironment {

    private static final ILogger LOGGER = Logger.getLogger(JobExecutionEnvironment.class);

    private final JobConfig jobConfig;

    private final int maxParallelism = 1;

    private final List<Action> actions = new ArrayList<>();

    private final Set<URL> jarUrls = new HashSet<>();

    private final List<URL> commonPluginJars = new ArrayList<>();

    private final String jobFilePath;

    private final IdGenerator idGenerator;

    private final SeaTunnelHazelcastClient seaTunnelHazelcastClient;

    private final JobClient jobClient;

    public JobExecutionEnvironment(JobConfig jobConfig,
                                   String jobFilePath,
                                   SeaTunnelHazelcastClient seaTunnelHazelcastClient) {
        this.jobConfig = jobConfig;
        this.jobFilePath = jobFilePath;
        this.idGenerator = new IdGenerator();
        this.seaTunnelHazelcastClient = seaTunnelHazelcastClient;
        this.jobClient = new JobClient(seaTunnelHazelcastClient);
        this.jobConfig.setJobContext(new JobContext(jobClient.getNewJobId()));
        this.commonPluginJars.addAll(searchPluginJars());
        this.commonPluginJars.addAll(new ArrayList<>(Common.getThirdPartyJars(jobConfig.getEnvOptions()
                .getOrDefault(EnvCommonOptions.JARS.key(), "").toString()).stream().map(Path::toUri)
            .map(uri -> {
                try {
                    return uri.toURL();
                } catch (MalformedURLException e) {
                    throw new SeaTunnelEngineException("the uri of jar illegal:" + uri, e);
                }
            })
            .collect(Collectors.toList())));
        LOGGER.info("add common jar in plugins :" + commonPluginJars);
    }

    /**
     * Search all jars in SEATUNNEL_HOME/plugins
     */
    private Set<URL> searchPluginJars() {
        try {
            if (Files.exists(Common.pluginRootDir())) {
                return new HashSet<>(FileUtils.searchJarFiles(Common.pluginRootDir()));
            }
        } catch (IOException | SeaTunnelEngineException e) {
            LOGGER.warning(String.format("Can't search plugin jars in %s.", Common.pluginRootDir()), e);
        }
        return Collections.emptySet();
    }

    private JobConfigParser getJobConfigParser() {
        return new JobConfigParser(jobFilePath, idGenerator, jobConfig, commonPluginJars);
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
            new ArrayList<>(jarUrls));

        return jobClient.createJobProxy(jobImmutableInformation);
    }

    public LogicalDag getLogicalDag() {
        ImmutablePair<List<Action>, Set<URL>> immutablePair = getJobConfigParser().parse();
        actions.addAll(immutablePair.getLeft());
        jarUrls.addAll(immutablePair.getRight());
        return getLogicalDagGenerator().generate();
    }
}

