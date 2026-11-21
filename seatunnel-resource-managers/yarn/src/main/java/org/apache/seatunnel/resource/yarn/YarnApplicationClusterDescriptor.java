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

import org.apache.seatunnel.shade.com.google.common.io.Closer;

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
import org.apache.seatunnel.resource.yarn.client.YarnClientRequest;
import org.apache.seatunnel.resource.yarn.config.YarnApplicationConfiguration;
import org.apache.seatunnel.resource.yarn.config.YarnConfigurationUtils;
import org.apache.seatunnel.resource.yarn.config.YarnDeploymentTarget;
import org.apache.seatunnel.resource.yarn.config.YarnOptions;
import org.apache.seatunnel.resource.yarn.config.YarnSecurityContext;
import org.apache.seatunnel.resource.yarn.exception.YarnClientException;
import org.apache.seatunnel.resource.yarn.launch.YarnApplicationFileUploader;
import org.apache.seatunnel.resource.yarn.launch.YarnLocalResourceDescriptor;
import org.apache.seatunnel.resource.yarn.launch.YarnStagingDirectory;
import org.apache.seatunnel.resource.yarn.launch.factory.ApplicationMasterContainerFactory;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnContainerSpec;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnResourceRequirements;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.client.config.ClientConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Submits one distribution and one job as a private, single-attempt YARN application. */
public final class YarnApplicationClusterDescriptor implements ClusterDescriptor<ApplicationId> {
    private static final Logger LOG =
            LoggerFactory.getLogger(YarnApplicationClusterDescriptor.class);

    private final Configuration configuration;
    private final Supplier<YarnClient> clientFactory;
    private final boolean allowLocalStaging;
    private YarnClient yarnClient;
    private final ReadonlyConfig options;
    private final YarnSecurityContext security;

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
            boolean allowLocalStaging,
            Map<String, String> options) {
        this.options = ReadonlyConfig.fromMap(new HashMap<>(options));
        this.allowLocalStaging = allowLocalStaging;
        this.clientFactory = clientFactory;
        this.configuration = new Configuration(configuration);
        this.security = new YarnSecurityContext(this.configuration, this.options);
    }

    /**
     * Stages the distribution and job under the YARN application ID and submits a single AM attempt
     * using the submitter's Hadoop identity.
     *
     * @param specification resolved job and container requirements
     * @return the RM-assigned ID once the AM is running or the application has already succeeded
     * @throws Exception if staging, submission or waiting for startup fails
     */
    @Override
    public ApplicationId deployApplication(ApplicationSpecification specification)
            throws Exception {
        return security.run(() -> submit(specification));
    }

    private ApplicationId submit(ApplicationSpecification specification) throws Exception {
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
            YarnClientApplication application =
                    YarnClientRequest.call(
                            "Could not create YARN application " + specification.getName(),
                            client::createApplication);
            ApplicationSubmissionContext allocated = application.getApplicationSubmissionContext();
            yarnId = allocated.getApplicationId();
            LOG.info("Submitting YARN application {}", yarnId);
            ApplicationMasterContainerFactory.validateResources(
                    specification,
                    application.getNewApplicationResponse().getMaximumResourceCapability());

            YarnContainerSpec master;
            Closer closer = Closer.create();
            try {
                YarnApplicationFileUploader uploader =
                        closer.register(
                                new YarnApplicationFileUploader(
                                        configuration, deployment, yarnId, allowLocalStaging));
                YarnLocalResourceDescriptor resources = uploader.upload();
                staging = uploader.getApplicationDir();
                master =
                        ApplicationMasterContainerFactory.create(
                                configuration,
                                staging,
                                YarnResourceRequirements.forMaster(specification),
                                resources,
                                deployment.getHadoopUserName());
            } catch (Throwable failure) {
                throw closer.rethrow(failure, Exception.class);
            } finally {
                closer.close();
            }
            ApplicationSubmissionContext submission =
                    ApplicationMasterContainerFactory.createSubmission(
                            allocated, deployment, master);
            // A lost submit response can still mean the RM accepted the application.
            submitted = true;
            YarnClientRequest.call(
                    "Could not submit YARN application " + yarnId,
                    () -> client.submitApplication(submission));
            new YarnApplicationStatusMonitor(client)
                    .awaitRunning(yarnId, specification.getStartupTimeoutMillis());
            LOG.info("YARN application {} master is running", yarnId);
            return yarnId;
        } catch (Exception failure) {
            cleanupFailedSubmission(client, yarnId, staging, submitted, failure);
            if (failure instanceof IOException) {
                throw new YarnClientException(
                        "Could not prepare YARN application " + yarnId, failure);
            }
            throw failure;
        }
    }

    /**
     * Kills a possibly accepted application and removes only resources created by this attempt.
     *
     * @param client YARN client used to kill the application
     * @param yarnId application identifier, or {@code null} before it was assigned
     * @param staging staging directory, or {@code null} before upload completed
     * @param submitted whether submitApplication has been called and may have been accepted
     * @param failure original submission failure receiving suppressed cleanup failures
     */
    private void cleanupFailedSubmission(
            YarnClient client,
            ApplicationId yarnId,
            Path staging,
            boolean submitted,
            Exception failure) {
        if (submitted) {
            try {
                client.killApplication(yarnId);
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
        }
        if (staging != null) {
            try {
                YarnStagingDirectory.cleanup(
                        YarnConfigurationUtils.withBoundedRpc(configuration), staging);
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
        }
        try {
            close();
        } catch (RuntimeException cleanup) {
            failure.addSuppressed(cleanup);
        }
        if (failure instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Looks up the running AM's registered endpoint. The returned provider opens the engine
     * connection when its caller requests a client.
     *
     * @param applicationId native YARN application ID printed at submission
     * @return a client provider bound to this application's Hazelcast cluster
     * @throws Exception if the AM has no live endpoint or the startup deadline expires
     */
    @Override
    public SeatunnelClientProvider retrieve(ApplicationId applicationId) throws Exception {
        return security.run(() -> retrieveClient(applicationId));
    }

    private SeatunnelClientProvider retrieveClient(ApplicationId applicationId) throws Exception {
        long timeout = options.get(ApplicationOptions.STARTUP_TIMEOUT_MILLIS);
        ApplicationReport report =
                new YarnApplicationStatusMonitor(yarnClient()).awaitRunning(applicationId, timeout);
        if (report.getYarnApplicationState() != YarnApplicationState.RUNNING) {
            throw new IllegalStateException(
                    "YARN application " + applicationId + " has no live master");
        }
        return createClientProvider(applicationId, report, timeout);
    }

    /**
     * Queries the RM as the submitter. Observing a terminal result also triggers staging cleanup.
     */
    @Override
    public ApplicationStatus getApplicationStatus(ApplicationId applicationId) throws Exception {
        return security.run(() -> application(applicationId).getStatus());
    }

    /**
     * Asks the RM to kill the application and then removes the application's private staging
     * directory.
     */
    @Override
    public void cancelApplication(ApplicationId applicationId) throws Exception {
        security.run(
                () -> {
                    application(applicationId).cancel();
                    return null;
                });
    }

    private YarnApplicationClient application(ApplicationId applicationId) {
        Path staging =
                new Path(
                        new Path(options.get(YarnOptions.STAGING_DIRECTORY)),
                        applicationId.toString());
        return new YarnApplicationClient(
                yarnClient(),
                YarnConfigurationUtils.withBoundedRpc(configuration),
                applicationId,
                staging);
    }

    private SeatunnelClientProvider createClientProvider(
            ApplicationId id, ApplicationReport report, long timeout) {
        String host = report.getHost();
        int port = report.getRpcPort();
        if (host == null || host.trim().isEmpty() || port <= 0) {
            throw new IllegalStateException(
                    "YARN application " + id + " has no live master endpoint");
        }
        String address = masterAddress(host, port);
        ClientConfig config = ConfigProvider.locateAndGetClientConfig();
        config.setClusterName(SeatunnelApplicationConfig.clusterName(id.toString()));
        config.getNetworkConfig().setAddresses(Collections.singletonList(address));
        config.getConnectionStrategyConfig()
                .getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(timeout);
        return () -> new SeaTunnelClient(config);
    }

    private static String masterAddress(String host, int port) {
        String formattedHost =
                host.contains(":") && !host.startsWith("[") ? "[" + host + "]" : host;
        return formattedHost + ":" + port;
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
            throw new YarnClientException("Could not start YARN client", failure);
        }
    }

    /** Releases the local YARN RPC client. A detached application continues running. */
    @Override
    public void close() {
        if (yarnClient != null) {
            YarnClient client = yarnClient;
            yarnClient = null;
            try {
                client.stop();
            } catch (RuntimeException failure) {
                throw new YarnClientException("Could not close YARN client", failure);
            }
        }
    }
}
