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

package org.apache.seatunnel.resource.yarn.cli;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;
import org.apache.seatunnel.engine.client.job.ApplicationJobExecutionEnvironment;
import org.apache.seatunnel.engine.common.config.ApplicationClusterConfig;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.classloader.JarPathResolver;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ApplicationResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.thirdparty.yarn.YarnResourceManagerFactory;
import org.apache.seatunnel.resource.yarn.YarnResourceManagerDriverFactory;
import org.apache.seatunnel.resource.yarn.config.YarnConfigurationUtils;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.launch.YarnStagingDirectory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Runs the YARN application master and its single native job. */
public final class SeatunnelYarnMasterCli {

    private static final Logger LOG = LoggerFactory.getLogger(SeatunnelYarnMasterCli.class);

    /** Runs the native job and releases staged artifacts on normal or interrupted shutdown. */
    public static void main(String[] args) {
        int exitCode = 0;
        try {
            runApplication();
        } catch (Exception failure) {
            LOG.error("YARN application failed", failure);
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    private static void runApplication() throws Exception {
        Configuration configuration =
                YarnConfigurationUtils.loadLocalized(YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME);
        Path staging = YarnStagingDirectory.fromEnvironment();
        Thread cleanup =
                new Thread(
                        () -> {
                            try {
                                YarnStagingDirectory.cleanup(configuration, staging);
                            } catch (Exception failure) {
                                LOG.warn(
                                        "Could not remove application staging directory {}",
                                        staging,
                                        failure);
                            }
                        },
                        "seatunnel-yarn-staging-cleanup");
        Runtime.getRuntime().addShutdownHook(cleanup);
        try (AutoCloseable stagedArtifacts =
                () -> YarnStagingDirectory.cleanup(configuration, staging)) {
            String container = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
            String id =
                    ContainerId.fromString(container)
                            .getApplicationAttemptId()
                            .getApplicationId()
                            .toString();
            ApplicationSpecification specification =
                    ApplicationSpecification.read(
                            Paths.get(YarnConstants.LOCALIZED_SPECIFICATION_NAME));
            SeaTunnelConfig engineConfiguration = ConfigProvider.locateAndGetSeaTunnelConfig();
            String clusterName = ApplicationClusterConfig.clusterName(id);
            ApplicationClusterConfig.configure(
                    engineConfiguration,
                    clusterName,
                    null,
                    specification.getWorkerSpecification().getSlots());
            ApplicationClusterConfig.configureCheckpointRetention(engineConfiguration);
            engineConfiguration
                    .getHazelcastConfig()
                    .getNetworkConfig()
                    .setPort(specification.getOption(ApplicationOptions.MASTER_PORT))
                    .setPortAutoIncrement(true);
            ResourceManagerDriver<?> driver =
                    new YarnResourceManagerDriverFactory().create(specification, clusterName);
            HazelcastInstanceImpl master;
            try {
                master =
                        SeaTunnelServerStarter.createHazelcastInstance(
                                engineConfiguration,
                                null,
                                JarPathResolver.identity(),
                                new YarnResourceManagerFactory<>(id, specification, driver));
            } catch (Exception failure) {
                try {
                    driver.close();
                } catch (Exception e) {
                    failure.addSuppressed(e);
                }
                throw failure;
            }
            SeaTunnelServer server =
                    master.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
            Thread owner = Thread.currentThread();
            CountDownLatch stopped = new CountDownLatch(1);
            Thread shutdown =
                    new Thread(
                            () -> {
                                owner.interrupt();
                                try {
                                    stopped.await(155, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            },
                            "seatunnel-yarn-application-shutdown");
            try (AutoCloseable runtime =
                    () -> {
                        boolean interrupted = Thread.interrupted();
                        try {
                            if (server.getCoordinatorService().getInitializedResourceManager()
                                    == null) {
                                driver.close();
                            }
                        } finally {
                            CompletableFuture.runAsync(master::shutdown).join();
                            if (interrupted) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }) {
                Runtime.getRuntime().addShutdownHook(shutdown);
                executeJob(server, specification);
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdown);
                } catch (IllegalStateException ignored) {
                    // The VM is already executing this hook.
                }
                stopped.countDown();
            }
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(cleanup);
            } catch (IllegalStateException ignored) {
                // The VM already started the hook, which performs the same idempotent cleanup.
            }
        }
    }

    /** Runs the job between resource readiness and resource cleanup; the caller owns the master. */
    private static void executeJob(SeaTunnelServer server, ApplicationSpecification specification)
            throws Exception {
        ApplicationResourceManager<?> resources =
                (ApplicationResourceManager<?>) server.getCoordinatorService().getResourceManager();
        CompletableFuture<Void> cancellation = new CompletableFuture<>();
        CompletableFuture<JobResult> execution = null;
        JobResult result = null;
        Exception failure = null;
        try {
            resources.awaitWorkerRegistration();
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(specification.getName());
            execution =
                    new ApplicationJobExecutionEnvironment(
                                    jobConfig,
                                    ConfigShadeUtils.decryptConfig(
                                            ConfigFactory.parseString(specification.getJobConfig())
                                                    .resolve()),
                                    server,
                                    specification.getJobId(),
                                    specification.getOption(ApplicationOptions.RESTORE_JOB_ID))
                            .execute(cancellation);
            result =
                    (JobResult)
                            CompletableFuture.anyOf(execution, resources.getFailureFuture()).get();
        } catch (Exception e) {
            failure = e;
        } finally {
            boolean interrupted = Thread.interrupted() || failure instanceof InterruptedException;
            try {
                if (execution != null && !execution.isDone()) {
                    cancellation.complete(null);
                    try {
                        execution.get(10, TimeUnit.SECONDS);
                    } catch (Exception cleanup) {
                        if (failure == null) {
                            failure = cleanup;
                        } else {
                            failure.addSuppressed(cleanup);
                        }
                    }
                }
                resources.finishApplication(result, failure);
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
