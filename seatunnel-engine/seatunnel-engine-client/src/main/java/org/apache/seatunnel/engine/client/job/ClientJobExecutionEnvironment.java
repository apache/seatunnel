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
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.AbstractJobEnvironment;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ClientJobExecutionEnvironment extends AbstractJobEnvironment {

    private final String jobFilePath;

    private final SeaTunnelHazelcastClient seaTunnelHazelcastClient;

    private final JobClient jobClient;

    private final SeaTunnelConfig seaTunnelConfig;

    private final ConnectorPackageClient connectorPackageClient;

    /** If the JobId is not empty, it is used to restore job from savePoint */
    public ClientJobExecutionEnvironment(
            JobConfig jobConfig,
            String jobFilePath,
            SeaTunnelHazelcastClient seaTunnelHazelcastClient,
            SeaTunnelConfig seaTunnelConfig,
            boolean isStartWithSavePoint,
            Long jobId) {
        super(jobConfig, isStartWithSavePoint);
        this.jobFilePath = jobFilePath;
        this.seaTunnelHazelcastClient = seaTunnelHazelcastClient;
        this.jobClient = new JobClient(seaTunnelHazelcastClient);
        this.seaTunnelConfig = seaTunnelConfig;
        this.jobConfig.setJobContext(
                new JobContext(isStartWithSavePoint ? jobId : jobClient.getNewJobId()));
        this.connectorPackageClient = new ConnectorPackageClient(seaTunnelHazelcastClient);
    }

    public ClientJobExecutionEnvironment(
            JobConfig jobConfig,
            String jobFilePath,
            SeaTunnelHazelcastClient seaTunnelHazelcastClient,
            SeaTunnelConfig seaTunnelConfig) {
        this(jobConfig, jobFilePath, seaTunnelHazelcastClient, seaTunnelConfig, false, null);
    }

    /** Search all jars in SEATUNNEL_HOME/plugins */
    @Override
    protected MultipleTableJobConfigParser getJobConfigParser() {
        return new MultipleTableJobConfigParser(
                jobFilePath, idGenerator, jobConfig, commonPluginJars, isStartWithSavePoint);
    }

    @Override
    protected LogicalDag getLogicalDag() {
        ImmutablePair<List<Action>, Set<URL>> immutablePair = getJobConfigParser().parse();
        actions.addAll(immutablePair.getLeft());
        // Enable upload connector jar package to engine server, automatically upload connector Jar
        // packages and dependent third-party Jar packages to the server before job execution.
        // Enabling this configuration does not require the server to hold all connector Jar
        // packages.
        boolean enableUploadConnectorJarPackage =
                seaTunnelConfig.getEngineConfig().getConnectorJarStorageConfig().getEnable();
        if (enableUploadConnectorJarPackage) {
            Set<ConnectorJarIdentifier> commonJarIdentifiers =
                    connectorPackageClient.uploadCommonPluginJars(
                            Long.parseLong(jobConfig.getJobContext().getJobId()), commonPluginJars);
            Set<URL> commonPluginJarUrls = getJarUrlsFromIdentifiers(commonJarIdentifiers);
            Set<ConnectorJarIdentifier> pluginJarIdentifiers = new HashSet<>();
            uploadActionPluginJar(actions, pluginJarIdentifiers);
            Set<URL> connectorPluginJarUrls = getJarUrlsFromIdentifiers(pluginJarIdentifiers);
            connectorJarIdentifiers.addAll(commonJarIdentifiers);
            connectorJarIdentifiers.addAll(pluginJarIdentifiers);
            jarUrls.addAll(commonPluginJarUrls);
            jarUrls.addAll(connectorPluginJarUrls);
            actions.forEach(
                    action -> {
                        addCommonPluginJarsToAction(
                                action, commonPluginJarUrls, commonJarIdentifiers);
                    });
        } else {
            jarUrls.addAll(commonPluginJars);
            jarUrls.addAll(immutablePair.getRight());
            actions.forEach(
                    action -> {
                        addCommonPluginJarsToAction(
                                action, new HashSet<>(commonPluginJars), Collections.emptySet());
                    });
        }
        return getLogicalDagGenerator().generate();
    }

    protected Set<ConnectorJarIdentifier> uploadPluginJars(Set<URL> pluginJarUrls) {
        Set<ConnectorJarIdentifier> pluginJarIdentifiers = new HashSet<>();
        pluginJarUrls.forEach(
                pluginJarUrl -> {
                    ConnectorJarIdentifier connectorJarIdentifier =
                            connectorPackageClient.uploadConnectorPluginJar(
                                    Long.parseLong(jobConfig.getJobContext().getJobId()),
                                    pluginJarUrl);
                    pluginJarIdentifiers.add(connectorJarIdentifier);
                });
        return pluginJarIdentifiers;
    }

    private void uploadActionPluginJar(List<Action> actions, Set<ConnectorJarIdentifier> result) {
        actions.forEach(
                action -> {
                    Set<URL> jarUrls = action.getJarUrls();
                    Set<ConnectorJarIdentifier> jarIdentifiers = uploadPluginJars(jarUrls);
                    result.addAll(jarIdentifiers);
                    // Reset the client URL of the jar package in Set
                    // add the URLs from remote master node
                    jarUrls.clear();
                    jarUrls.addAll(getJarUrlsFromIdentifiers(jarIdentifiers));
                    action.getConnectorJarIdentifiers().addAll(jarIdentifiers);
                    if (!action.getUpstream().isEmpty()) {
                        uploadActionPluginJar(action.getUpstream(), result);
                    }
                });
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
}
