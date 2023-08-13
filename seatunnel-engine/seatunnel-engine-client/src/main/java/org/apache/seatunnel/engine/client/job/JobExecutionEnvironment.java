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
import org.apache.seatunnel.engine.core.dag.actions.Config;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDagGenerator;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class JobExecutionEnvironment {

    private static final ILogger LOGGER = Logger.getLogger(JobExecutionEnvironment.class);

    private final boolean isStartWithSavePoint;

    private final JobConfig jobConfig;

    private final List<Action> actions = new ArrayList<>();

    private final Set<URL> jarUrls = new HashSet<>();

    private final Set<ConnectorJarIdentifier> connectorJarIdentifiers = new HashSet<>();

    private final List<URL> commonPluginJars = new ArrayList<>();

    private final String jobFilePath;

    private final IdGenerator idGenerator;

    private final SeaTunnelHazelcastClient seaTunnelHazelcastClient;

    private final JobClient jobClient;

    private final ConnectorPackageClient connectorPackageClient;

    /** If the JobId is not empty, it is used to restore job from savePoint */
    public JobExecutionEnvironment(
            JobConfig jobConfig,
            String jobFilePath,
            SeaTunnelHazelcastClient seaTunnelHazelcastClient,
            boolean isStartWithSavePoint,
            Long jobId) {
        this.jobConfig = jobConfig;
        this.jobFilePath = jobFilePath;
        this.idGenerator = new IdGenerator();
        this.seaTunnelHazelcastClient = seaTunnelHazelcastClient;
        this.jobClient = new JobClient(seaTunnelHazelcastClient);
        this.isStartWithSavePoint = isStartWithSavePoint;
        this.jobConfig.setJobContext(
                new JobContext(isStartWithSavePoint ? jobId : jobClient.getNewJobId()));
        this.commonPluginJars.addAll(searchPluginJars());
        this.commonPluginJars.addAll(
                new ArrayList<>(
                        Common.getThirdPartyJars(
                                jobConfig
                                        .getEnvOptions()
                                        .getOrDefault(EnvCommonOptions.JARS.key(), "")
                                        .toString())
                                .stream()
                                .map(Path::toUri)
                                .map(
                                        uri -> {
                                            try {
                                                return uri.toURL();
                                            } catch (MalformedURLException e) {
                                                throw new SeaTunnelEngineException(
                                                        "the uri of jar illegal:" + uri, e);
                                            }
                                        })
                                .collect(Collectors.toList())));
        LOGGER.info("add common jar in plugins :" + commonPluginJars);
        this.connectorPackageClient = new ConnectorPackageClient(seaTunnelHazelcastClient);
    }

    public JobExecutionEnvironment(
            JobConfig jobConfig,
            String jobFilePath,
            SeaTunnelHazelcastClient seaTunnelHazelcastClient) {
        this(jobConfig, jobFilePath, seaTunnelHazelcastClient, false, null);
    }

    /** Search all jars in SEATUNNEL_HOME/plugins */
    private Set<URL> searchPluginJars() {
        try {
            if (Files.exists(Common.pluginRootDir())) {
                return new HashSet<>(FileUtils.searchJarFiles(Common.pluginRootDir()));
            }
        } catch (IOException | SeaTunnelEngineException e) {
            LOGGER.warning(
                    String.format("Can't search plugin jars in %s.", Common.pluginRootDir()), e);
        }
        return Collections.emptySet();
    }

    private MultipleTableJobConfigParser getJobConfigParser() {
        return new MultipleTableJobConfigParser(
                jobFilePath, idGenerator, jobConfig, commonPluginJars);
    }

    private LogicalDagGenerator getLogicalDagGenerator() {
        return new LogicalDagGenerator(actions, jobConfig, idGenerator);
    }

    public ClientJobProxy execute() throws ExecutionException, InterruptedException {
        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        Long.parseLong(jobConfig.getJobContext().getJobId()),
                        jobConfig.getName(),
                        isStartWithSavePoint,
                        seaTunnelHazelcastClient.getSerializationService().toData(getLogicalDag()),
                        jobConfig,
                        new ArrayList<>(jarUrls),
                        new ArrayList<>(connectorJarIdentifiers));

        return jobClient.createJobProxy(jobImmutableInformation);
    }

    private LogicalDag getLogicalDag() {
        ImmutablePair<List<Action>, Set<URL>>
                immutablePair = getJobConfigParser().parse();
        actions.addAll(immutablePair.getLeft());

        Set<ConnectorJarIdentifier> commonJarIdentifiers =
                connectorPackageClient.uploadCommonPluginJars(
                        Long.parseLong(jobConfig.getJobContext().getJobId()), commonPluginJars);
        Set<URL> commonPluginJarUrls = getJarUrlsFromIdentifiers(commonJarIdentifiers);
        Set<ConnectorJarIdentifier> pluginJarIdentifiers = new HashSet<>();
        transformActionPluginJarUrls(actions, pluginJarIdentifiers);
        Set<URL> connectorPluginJarUrls = getJarUrlsFromIdentifiers(pluginJarIdentifiers);
        connectorJarIdentifiers.addAll(commonJarIdentifiers);
        connectorJarIdentifiers.addAll(pluginJarIdentifiers);
        jarUrls.addAll(commonPluginJars);
        jarUrls.addAll(connectorPluginJarUrls);
        actions.forEach(action -> {
            addCommonPluginJarsToAction(action, commonPluginJarUrls, commonJarIdentifiers);
        });
        actions.forEach(
                action -> {
                    Config config = action.getConfig();
                });
        return getLogicalDagGenerator().generate();
    }

    void addCommonPluginJarsToAction(Action action, Set<URL> commonPluginJars, Set<ConnectorJarIdentifier> commonJarIdentifiers) {
        action.getJarUrls().addAll(commonPluginJars);
        action.getConnectorJarIdentifiers().addAll(commonJarIdentifiers);
        if (!action.getUpstream().isEmpty()) {
            action.getUpstream().forEach(upstreamAction -> {
                addCommonPluginJarsToAction(upstreamAction, commonPluginJars, commonJarIdentifiers);
            });
        }
    }

    private void transformActionPluginJarUrls(List<Action> actions, Set<ConnectorJarIdentifier> result) {
        actions.forEach(
                action -> {
                    Set<URL> jarUrls = action.getJarUrls();
                    Set<ConnectorJarIdentifier> jarIdentifiers = uploadPluginJarUrls(jarUrls);
                    result.addAll(jarIdentifiers);
                    // Reset the client URL of the jar package in Set
                    // add the URLs from remote master node
                    jarUrls.clear();
                    jarUrls.addAll(getJarUrlsFromIdentifiers(jarIdentifiers));
                    action.getConnectorJarIdentifiers().addAll(jarIdentifiers);
                    if (!action.getUpstream().isEmpty()) {
                        transformActionPluginJarUrls(action.getUpstream(), result);
                    }
                });
    }

    private Set<ConnectorJarIdentifier> uploadPluginJarUrls(Set<URL> pluginJarUrls) {
        Set<ConnectorJarIdentifier> pluginJarIdentifiers = new HashSet<>();
        pluginJarUrls.forEach(
                pluginJarUrl -> {
                    ConnectorJarIdentifier connectorJarIdentifier = connectorPackageClient.uploadConnectorPluginJar(
                            Long.parseLong(jobConfig.getJobContext().getJobId()),
                            pluginJarUrl);
                    pluginJarIdentifiers.add(connectorJarIdentifier);
                });
        return pluginJarIdentifiers;
    }

    private Set<URL> getJarUrlsFromIdentifiers(Set<ConnectorJarIdentifier> connectorJarIdentifiers) {
        Set<URL> jarUrls = new HashSet<>();
        connectorJarIdentifiers.stream().map(connectorJarIdentifier -> {
            File storageFile = new File(connectorJarIdentifier.getStoragePath());
            try {
                return Optional.of(storageFile.toURI().toURL());
            } catch (MalformedURLException e) {
                LOGGER.warning(String.format("Cannot get plugin URL: {%s}", storageFile));
                return Optional.empty();
            }
        }).collect(Collectors.toList()).forEach(optional -> {
            if (optional.isPresent()) {
                jarUrls.add((URL) optional.get());
            }
        });
        return jarUrls;
    }
}
