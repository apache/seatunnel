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

package org.apache.seatunnel.resource.yarn;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.deployment.ClusterDescriptor;
import org.apache.seatunnel.engine.client.deployment.SeatunnelClientProvider;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.resource.yarn.client.YarnApplicationClient;
import org.apache.seatunnel.resource.yarn.client.YarnApplicationStatusMonitor;
import org.apache.seatunnel.resource.yarn.config.YarnApplicationConfiguration;
import org.apache.seatunnel.resource.yarn.config.YarnConfigurationUtils;
import org.apache.seatunnel.resource.yarn.config.YarnDeploymentTarget;
import org.apache.seatunnel.resource.yarn.config.YarnOptions;
import org.apache.seatunnel.resource.yarn.launch.YarnApplicationFileUploader;
import org.apache.seatunnel.resource.yarn.launch.YarnContainerLaunchContextFactory;
import org.apache.seatunnel.resource.yarn.launch.YarnLocalResourceDescriptor;
import org.apache.seatunnel.resource.yarn.launch.YarnStagingDirectory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;

import com.hazelcast.client.config.ClientConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Submits one distribution and one job as a private, single-attempt YARN application. */
public final class YarnApplicationClusterDescriptor implements ClusterDescriptor<ApplicationId> {
    /** YARN application type shown by the ResourceManager UI and CLI. */
    private static final String APPLICATION_TYPE = "SeaTunnel";

    private final Configuration configuration;
    private final Supplier<YarnClient> clientFactory;
    private final boolean allowLocalStaging;
    private YarnClient yarnClient;
    private final ReadonlyConfig options;

    /** Uses the submitting user's Hadoop configuration for YARN RPCs and shared staging. */
    public YarnApplicationClusterDescriptor(Configuration configuration) {
        this(configuration, Collections.emptyMap());
    }

    public YarnApplicationClusterDescriptor(
            Configuration configuration, Map<String, String> options) {
        this(configuration, YarnClient::createYarnClient, false, options);
    }

    public YarnApplicationClusterDescriptor(
            Configuration configuration,
            Supplier<YarnClient> clientFactory,
            boolean allowLocalStaging) {
        this(configuration, clientFactory, allowLocalStaging, Collections.emptyMap());
    }

    public YarnApplicationClusterDescriptor(
            Configuration configuration,
            Supplier<YarnClient> clientFactory,
            boolean allowLocalStaging,
            Map<String, String> options) {
        this.options = ReadonlyConfig.fromMap(new HashMap<String, Object>(options));
        this.allowLocalStaging = allowLocalStaging;
        this.clientFactory = clientFactory;
        YarnConfigurationUtils.requireSimpleAuthentication(configuration);
        this.configuration = YarnConfigurationUtils.withBoundedRpc(configuration);
    }

    /**
     * Stages a private distribution and job, then submits exactly one non-restarting AM. A
     * submission failure kills a possibly accepted application and deletes only files created here.
     */
    @Override
    public ApplicationId deployApplication(ApplicationSpecification specification)
            throws Exception {
        YarnApplicationConfiguration deployment =
                YarnApplicationConfiguration.forSubmission(specification, options);
        if (deployment.getDeploymentTarget() != YarnDeploymentTarget.APPLICATION) {
            throw new UnsupportedOperationException(
                    "Unsupported YARN deployment target: " + deployment.getDeploymentTarget());
        }
        YarnClient client = yarnClient();
        Path staging = null;
        ApplicationId yarnId = null;
        boolean submitted = false;
        try {
            YarnClientApplication application = client.createApplication();
            ApplicationSubmissionContext submission = application.getApplicationSubmissionContext();
            yarnId = submission.getApplicationId();
            int masterMemory = specification.getMasterMemoryMb();
            int masterCores = specification.getMasterCpuCores();
            Resource maximum =
                    application.getNewApplicationResponse().getMaximumResourceCapability();
            if (masterMemory > maximum.getMemorySize()
                    || masterCores > maximum.getVirtualCores()
                    || specification.getWorkerSpecification().getMemoryMb()
                            > maximum.getMemorySize()
                    || specification.getWorkerSpecification().getCpuCores()
                            > maximum.getVirtualCores()) {
                throw new IllegalArgumentException(
                        "Requested master/worker resources exceed YARN maximum container capability");
            }
            submission.setApplicationName(specification.getName());
            submission.setApplicationType(APPLICATION_TYPE);
            submission.setQueue(deployment.getQueue());
            if (deployment.getPriority() >= 0) {
                submission.setPriority(Priority.newInstance(deployment.getPriority()));
            }
            if (!deployment.getTags().isEmpty()) {
                submission.setApplicationTags(deployment.getTags());
            }
            if (deployment.getMasterNodeLabel() != null) {
                submission.setNodeLabelExpression(deployment.getMasterNodeLabel());
            }
            submission.setMaxAppAttempts(1);
            submission.setResource(Resource.newInstance(masterMemory, masterCores));
            try (YarnApplicationFileUploader uploader =
                    new YarnApplicationFileUploader(
                            configuration,
                            deployment,
                            submission.getApplicationId(),
                            allowLocalStaging)) {
                YarnLocalResourceDescriptor resources = uploader.upload();
                staging = uploader.getApplicationDir();
                submission.setAMContainerSpec(
                        YarnContainerLaunchContextFactory.master(staging, masterMemory, resources));
            }
            // A lost submit response can still mean the RM accepted the application.
            submitted = true;
            client.submitApplication(submission);
            new YarnApplicationStatusMonitor(client)
                    .awaitRunning(yarnId, specification.getStartupTimeoutMillis());
            return yarnId;
        } catch (Exception failure) {
            if (submitted) {
                try {
                    client.killApplication(yarnId);
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            if (staging != null) {
                try {
                    YarnStagingDirectory.cleanup(configuration, staging);
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            try {
                close();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /** Discovers the master endpoint without creating an Engine client. */
    @Override
    public SeatunnelClientProvider retrieve(ApplicationId applicationId) throws Exception {
        long timeout = options.get(ApplicationOptions.STARTUP_TIMEOUT_MILLIS);
        ApplicationReport report =
                new YarnApplicationStatusMonitor(yarnClient()).awaitRunning(applicationId, timeout);
        if (report.getYarnApplicationState() != YarnApplicationState.RUNNING) {
            throw new IllegalStateException(
                    "YARN application " + applicationId + " has no live master");
        }
        return createClientProvider(applicationId, report, timeout);
    }

    @Override
    public ApplicationStatus getApplicationStatus(ApplicationId applicationId) throws Exception {
        return application(applicationId).getStatus();
    }

    @Override
    public void cancelApplication(ApplicationId applicationId) throws Exception {
        application(applicationId).cancel();
    }

    private YarnApplicationClient application(ApplicationId applicationId) {
        Path staging =
                new Path(
                        new Path(options.get(YarnOptions.STAGING_DIRECTORY)),
                        applicationId.toString());
        return new YarnApplicationClient(yarnClient(), configuration, applicationId, staging);
    }

    private SeatunnelClientProvider createClientProvider(
            ApplicationId id, ApplicationReport report, long timeout) {
        String host = report.getHost();
        int port = report.getRpcPort();
        if (host == null || host.trim().isEmpty() || port <= 0) {
            throw new IllegalStateException(
                    "YARN application " + id + " has no live master endpoint");
        }
        String address =
                (host.contains(":") && !host.startsWith("[") ? "[" + host + "]" : host)
                        + ":"
                        + port;
        ClientConfig config = ConfigProvider.locateAndGetClientConfig();
        config.setClusterName(SeatunnelApplicationConfig.clusterName(id.toString()));
        config.getNetworkConfig().setAddresses(Collections.singletonList(address));
        config.getConnectionStrategyConfig()
                .getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(timeout);
        return () -> new SeaTunnelClient(config);
    }

    private YarnClient yarnClient() {
        if (yarnClient != null) {
            return yarnClient;
        }
        YarnClient client = clientFactory.get();
        try {
            client.init(configuration);
            client.start();
            yarnClient = client;
            return client;
        } catch (RuntimeException failure) {
            try {
                client.stop();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    @Override
    public void close() {
        if (yarnClient != null) {
            YarnClient client = yarnClient;
            yarnClient = null;
            client.stop();
        }
    }
}
