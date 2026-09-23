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
import org.apache.seatunnel.resource.yarn.cluster.YarnContainerLaunch;
import org.apache.seatunnel.resource.yarn.cluster.YarnDistribution;
import org.apache.seatunnel.resource.yarn.cluster.YarnStagingDirectory;
import org.apache.seatunnel.resource.yarn.config.YarnConfigurationUtils;
import org.apache.seatunnel.resource.yarn.config.YarnOptions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.apache.hadoop.yarn.api.records.ApplicationId.fromString;

/** Submits one distribution and one job as a private, single-attempt YARN application. */
final class YarnApplicationDeployer implements ApplicationDeployer {
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
        String distributionPath = specification.getOption(YarnOptions.DISTRIBUTION);
        if (distributionPath == null || distributionPath.trim().isEmpty()) {
            throw new IllegalArgumentException("Required option yarn.distribution is missing");
        }
        File distribution = new File(distributionPath).getAbsoluteFile();
        if (!Files.isRegularFile(distribution.toPath())) {
            throw new IllegalArgumentException(
                    "yarn.distribution must be a readable local distribution archive: "
                            + distribution);
        }
        YarnDistribution layout = YarnDistribution.inspect(distribution);
        YarnClient client = newClient();
        Path staging = null;
        String yarnId = null;
        boolean submitted = false;
        boolean staged = false;
        try {
            YarnClientApplication application = client.createApplication();
            ApplicationSubmissionContext submission = application.getApplicationSubmissionContext();
            yarnId = submission.getApplicationId().toString();
            Path stagingRoot = new Path(specification.getOption(YarnOptions.STAGING_DIRECTORY));
            try (FileSystem fileSystem =
                    FileSystem.newInstance(stagingRoot.toUri(), configuration)) {
                if (!allowLocalStaging && "file".equals(fileSystem.getUri().getScheme())) {
                    throw new IllegalArgumentException(
                            "yarn.staging-dir must resolve to a shared filesystem such as HDFS; local file staging is not supported");
                }
                staging = fileSystem.makeQualified(new Path(stagingRoot, yarnId));
                if (fileSystem.exists(staging)) {
                    throw new IllegalStateException(
                            "Application staging directory already exists: " + staging);
                }
                if (!fileSystem.mkdirs(staging, new FsPermission((short) 0700))) {
                    throw new IOException(
                            "Could not create application staging directory " + staging);
                }
                staged = true;
                fileSystem.setPermission(staging, new FsPermission((short) 0700));
                layout.stage(fileSystem, staging, distribution);
                File localSpecification =
                        Files.createTempFile("seatunnel-yarn-", ".properties").toFile();
                try {
                    specification.write(localSpecification.toPath());
                    fileSystem.copyFromLocalFile(
                            new Path(localSpecification.toURI()),
                            new Path(staging, YarnContainerLaunch.SPECIFICATION));
                } finally {
                    Files.deleteIfExists(localSpecification.toPath());
                }
                try (FSDataOutputStream output =
                        fileSystem.create(
                                new Path(staging, YarnContainerLaunch.HADOOP_CONFIGURATION),
                                false)) {
                    configuration.writeXml(output);
                }
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
            submission.setApplicationType("SeaTunnel");
            submission.setQueue(specification.getOption(YarnOptions.QUEUE));
            submission.setMaxAppAttempts(1);
            submission.setResource(Resource.newInstance(masterMemory, masterCores));
            submission.setAMContainerSpec(
                    YarnContainerLaunch.master(configuration, staging, masterMemory));
            // A lost submit response can still mean the RM accepted the application.
            submitted = true;
            client.submitApplication(submission);
            awaitApplicationMaster(client, yarnId, specification.getStartupTimeoutMillis());
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

    private void awaitApplicationMaster(YarnClient client, String yarnId, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            ApplicationReport report = client.getApplicationReport(fromString(yarnId));
            if (report.getYarnApplicationState() == YarnApplicationState.RUNNING
                    || YarnApplicationClient.status(report).isTerminal()) {
                return;
            }
            if (System.nanoTime() >= deadline) {
                throw new TimeoutException(
                        "YARN application "
                                + yarnId
                                + " did not start its ApplicationMaster within "
                                + timeoutMillis
                                + " ms; check queue capacity and NodeManager resources");
            }
            Thread.sleep(Math.min(500, timeoutMillis));
        }
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
