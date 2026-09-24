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

package org.apache.seatunnel.resource.yarn.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.client.ApplicationClient;
import org.apache.seatunnel.resource.core.client.ApplicationDeployer;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;
import org.apache.seatunnel.resource.yarn.config.YarnApplicationConfiguration;
import org.apache.seatunnel.resource.yarn.config.YarnConfigurationUtils;
import org.apache.seatunnel.resource.yarn.config.YarnDeploymentTarget;
import org.apache.seatunnel.resource.yarn.config.YarnOptions;
import org.apache.seatunnel.resource.yarn.launch.YarnApplicationFileUploader;
import org.apache.seatunnel.resource.yarn.launch.YarnContainerLaunchContextFactory;
import org.apache.seatunnel.resource.yarn.launch.YarnDistribution;
import org.apache.seatunnel.resource.yarn.launch.YarnStagingDirectory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.hadoop.yarn.api.records.ApplicationId.fromString;

/** Submits one distribution and one job as a private, single-attempt YARN application. */
final class YarnApplicationDeployer implements ApplicationDeployer {
    /** YARN application type shown by the ResourceManager UI and CLI. */
    private static final String APPLICATION_TYPE = "SeaTunnel";

    /** Private staging directories must only be accessible by their submitting user. */
    private static final FsPermission STAGING_PERMISSION = new FsPermission((short) 0700);

    /** Scheme rejected for production staging because NodeManagers cannot share local files. */
    private static final String LOCAL_FILE_SCHEME = "file";

    private final Configuration configuration;
    private final Supplier<YarnClient> clientFactory;
    private final boolean allowLocalStaging;

    /** Uses the submitting user's Hadoop configuration for YARN RPCs and shared staging. */
    YarnApplicationDeployer(Configuration configuration) {
        this(configuration, YarnClient::createYarnClient, false);
    }

    YarnApplicationDeployer(
            Configuration configuration,
            Supplier<YarnClient> clientFactory,
            boolean allowLocalStaging) {
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
    public ApplicationClient deploy(ApplicationSpecification specification) throws Exception {
        if (specification.getDeployType() != DeployType.YARN) {
            throw new IllegalArgumentException(
                    "YARN deployer requires a YARN application specification");
        }
        YarnApplicationConfiguration deployment =
                YarnApplicationConfiguration.forSubmission(specification);
        if (deployment.getDeploymentTarget() != YarnDeploymentTarget.APPLICATION) {
            throw new UnsupportedOperationException(
                    "Unsupported YARN deployment target: " + deployment.getDeploymentTarget());
        }
        YarnDistribution layout = YarnDistribution.inspect(deployment.getDistribution());
        YarnClient client = newClient();
        Path staging = null;
        String yarnId = null;
        boolean submitted = false;
        boolean staged = false;
        try {
            YarnClientApplication application = client.createApplication();
            ApplicationSubmissionContext submission = application.getApplicationSubmissionContext();
            yarnId = submission.getApplicationId().toString();
            Path stagingRoot = deployment.getStagingRoot();
            try (FileSystem fileSystem =
                    FileSystem.newInstance(stagingRoot.toUri(), configuration)) {
                if (!allowLocalStaging
                        && LOCAL_FILE_SCHEME.equals(fileSystem.getUri().getScheme())) {
                    throw new IllegalArgumentException(
                            "yarn.staging-dir must resolve to a shared filesystem such as HDFS; local file staging is not supported");
                }
                staging = fileSystem.makeQualified(new Path(stagingRoot, yarnId));
                if (fileSystem.exists(staging)) {
                    throw new IllegalStateException(
                            "Application staging directory already exists: " + staging);
                }
                if (!fileSystem.mkdirs(staging, STAGING_PERMISSION)) {
                    throw new IOException(
                            "Could not create application staging directory " + staging);
                }
                staged = true;
                fileSystem.setPermission(staging, STAGING_PERMISSION);
                YarnApplicationFileUploader.upload(
                        fileSystem,
                        staging,
                        layout,
                        deployment.getDistribution(),
                        specification,
                        configuration);
            }
            int masterMemory = specification.getOption(ApplicationOptions.MASTER_MEMORY_MB);
            int masterCores = specification.getOption(ApplicationOptions.MASTER_CPU_CORES);
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
            submission.setAMContainerSpec(
                    YarnContainerLaunchContextFactory.master(configuration, staging, masterMemory));
            // A lost submit response can still mean the RM accepted the application.
            submitted = true;
            client.submitApplication(submission);
            new YarnApplicationStatusMonitor(client)
                    .awaitRunning(fromString(yarnId), specification.getStartupTimeoutMillis());
            return new YarnApplicationClient(client, configuration, yarnId, staging);
        } catch (Exception failure) {
            if (submitted && yarnId != null) {
                try {
                    client.killApplication(fromString(yarnId));
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            if (staged) {
                try {
                    YarnStagingDirectory.cleanup(configuration, staging);
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            try {
                client.stop();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /**
     * Reopens a platform handle without requiring the submitting client process to remain alive.
     */
    @Override
    public ApplicationClient retrieve(ApplicationId applicationId, Map<String, String> options)
            throws Exception {
        if (applicationId.getDeployType() != DeployType.YARN) {
            throw new IllegalArgumentException("Application is not a YARN application");
        }
        String yarnId = fromString(applicationId.getId()).toString();
        Path staging;
        Path stagingRoot =
                new Path(
                        ReadonlyConfig.fromMap(new HashMap<String, Object>(options))
                                .get(YarnOptions.STAGING_DIRECTORY));
        try (FileSystem fileSystem = FileSystem.newInstance(stagingRoot.toUri(), configuration)) {
            staging = fileSystem.makeQualified(new Path(stagingRoot, yarnId));
        }
        return new YarnApplicationClient(newClient(), configuration, yarnId, staging);
    }

    private YarnClient newClient() {
        YarnClient client = clientFactory.get();
        try {
            client.init(configuration);
            client.start();
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
    public void close() {}
}
