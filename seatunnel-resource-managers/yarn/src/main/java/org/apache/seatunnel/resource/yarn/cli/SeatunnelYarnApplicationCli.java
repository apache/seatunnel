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

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.config.server.ServerConfigOptions;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.application.ApplicationJobRunner;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.resource.yarn.YarnResourceManagerDriver;
import org.apache.seatunnel.resource.yarn.config.YarnApplicationConfiguration;
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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Runs the YARN application master and its single native job. */
public final class SeatunnelYarnApplicationCli {
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 155;

    private static final Logger LOG = LoggerFactory.getLogger(SeatunnelYarnApplicationCli.class);

    /** Runs the native job and releases staged artifacts on normal or interrupted shutdown. */
    public static void main(String[] args) {
        int exitCode = 0;
        try {
            Configuration configuration =
                    YarnConfigurationUtils.loadLocalized(
                            YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME);
            Path staging = YarnStagingDirectory.fromEnvironment();
            // Keep a shutdown hook as the fallback for asynchronous termination (SIGTERM, OOM
            // kill, or any exit that skips the finally below). On those paths the finally never
            // runs, so this hook is the only cleanup that executes. Without it the staging
            // directory — which still holds the resolved job configuration and possibly
            // credentials — would leak on HDFS after an unclean exit.
            Thread cleanup = stagingCleanupHook(configuration, staging);
            Runtime.getRuntime().addShutdownHook(cleanup);
            try {
                String id = applicationId();
                LOG.info("Running YARN application {}", id);
                YarnApplicationConfiguration applicationConfiguration =
                        YarnApplicationConfiguration.read(
                                Paths.get(YarnConstants.LOCALIZED_SPECIFICATION_NAME));
                ApplicationSpecification specification =
                        applicationConfiguration.getSpecification();
                LOG.debug("Loaded application specification {}", specification.getName());
                SeaTunnelConfig engineConfiguration = configureEngine(id, applicationConfiguration);
                ResourceManagerDriver<?> driver =
                        createDriver(
                                configuration,
                                staging,
                                applicationConfiguration,
                                clusterName(id),
                                trackingUrl(engineConfiguration.getEngineConfig().getHttpConfig()));
                HazelcastInstanceImpl master =
                        startMaster(engineConfiguration, specification, id, driver);
                LOG.info("Master started for YARN application {}", id);
                runJob(specification, master, driver);
            } finally {
                LOG.info("Job finished, removing staging directory {}", staging);
                // Remove staging eagerly now that the job has finished: the master and every
                // worker have already localized these files, so the HDFS copy is dead data.
                // Cleaning up here (rather than relying only on the hook) guarantees removal on a
                // normal exit, and releases both the disk quota and any resolved credentials the
                // staging directory still contains.
                cleanupStaging(configuration, staging);
                // Drop the hook so the eager cleanup above is not executed a second time during
                // the upcoming System.exit shutdown sequence. The hook remains registered only on
                // the abnormal path where this finally never runs.
                removeShutdownHook(cleanup);
            }
        } catch (Exception failure) {
            LOG.error("YARN application failed", failure);
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    /**
     * Creates the SIGTERM fallback hook that removes the staging directory.
     *
     * @param configuration localized Hadoop configuration
     * @param staging application-owned staging directory
     * @return JVM shutdown hook thread
     */
    private static Thread stagingCleanupHook(Configuration configuration, Path staging) {
        return new Thread(
                () -> cleanupStaging(configuration, staging), "seatunnel-yarn-staging-cleanup");
    }

    /**
     * Deletes the application staging directory, logging but not propagating failures.
     *
     * @param configuration localized Hadoop configuration
     * @param staging application-owned staging directory
     */
    private static void cleanupStaging(Configuration configuration, Path staging) {
        try {
            YarnStagingDirectory.cleanup(
                    YarnConfigurationUtils.withBoundedRpc(configuration), staging);
        } catch (Exception failure) {
            LOG.warn("Could not remove application staging directory {}", staging, failure);
        }
    }

    /**
     * Removes a shutdown hook, tolerating the VM already executing it.
     *
     * @param shutdown hook thread to remove
     */
    private static void removeShutdownHook(Thread shutdown) {
        try {
            Runtime.getRuntime().removeShutdownHook(shutdown);
        } catch (IllegalStateException ignored) {
            // The VM already started the hook, which performs the same idempotent cleanup.
        }
    }

    /**
     * Resolves the YARN application identifier from the container environment.
     *
     * @return native YARN application ID
     */
    private static String applicationId() {
        String container = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
        return ContainerId.fromString(container)
                .getApplicationAttemptId()
                .getApplicationId()
                .toString();
    }

    /**
     * Builds the isolated Hazelcast cluster name for one application.
     *
     * @param id native YARN application ID
     * @return cluster name shared only by this application's master and workers
     */
    private static String clusterName(String id) {
        return SeatunnelApplicationConfig.clusterName(id);
    }

    /**
     * Prepares the master's Engine and Hazelcast configuration.
     *
     * @param id native YARN application ID used for the cluster name
     * @param application resolved application fields and optional AM HTTP port
     * @return configuration ready for master startup
     */
    private static SeaTunnelConfig configureEngine(
            String id, YarnApplicationConfiguration application) {
        ApplicationSpecification specification = application.getSpecification();
        SeaTunnelConfig engineConfiguration = YarnConfigurationUtils.loadEngineConfig();
        HttpConfig http = engineConfiguration.getEngineConfig().getHttpConfig();
        SeatunnelApplicationConfig.configure(
                engineConfiguration,
                clusterName(id),
                null,
                specification.getWorkerSpecification().getSlots());
        if (application.getMasterHttpPort() != null) {
            // The common application setup disables web listeners. Keep the user's HTTP auth,
            // context path and request limits when enabling the AM's fixed endpoint.
            if (http == ServerConfigOptions.MasterServerConfigOptions.HTTP.defaultValue()) {
                http = new HttpConfig();
            }
            http.setEnabled(true);
            http.setEnableHttps(false);
            http.setEnableDynamicPort(false);
            http.setPort(application.getMasterHttpPort());
            engineConfiguration.getEngineConfig().setHttpConfig(http);
        }
        SeatunnelApplicationConfig.configureCheckpointRetention(engineConfiguration);
        engineConfiguration
                .getHazelcastConfig()
                .getNetworkConfig()
                .setPort(specification.getMasterPort())
                .setPortAutoIncrement(true);
        return engineConfiguration;
    }

    /** Builds the AM's original tracking URL on its NodeManager host for the YARN web proxy. */
    private static String trackingUrl(HttpConfig http) throws URISyntaxException {
        if (!http.isEnabled()) {
            return "";
        }
        String path = http.getContextPath();
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        if (!path.endsWith("/")) {
            path += "/";
        }
        return new URI(
                        "http",
                        null,
                        System.getenv(ApplicationConstants.Environment.NM_HOST.name()),
                        http.getPort(),
                        path,
                        null,
                        null)
                .toASCIIString();
    }

    /**
     * Creates the YARN resource manager driver with resolved placement and user settings.
     *
     * @param configuration localized Hadoop configuration
     * @param staging application-owned staging directory
     * @param applicationConfiguration resolved YARN settings
     * @param clusterName isolated Hazelcast cluster name
     * @param trackingUrl running AM web endpoint registered with the ResourceManager
     * @return driver used to allocate and release worker containers
     */
    private static ResourceManagerDriver<?> createDriver(
            Configuration configuration,
            Path staging,
            YarnApplicationConfiguration applicationConfiguration,
            String clusterName,
            String trackingUrl) {
        return new YarnResourceManagerDriver(
                configuration,
                staging,
                new YarnResourceManagerDriver.YarnDriverSettings(
                        clusterName,
                        applicationConfiguration.getWorkerNodeLabel(),
                        applicationConfiguration.getHadoopUserName(),
                        System.getProperty(YarnConstants.SEATUNNEL_HOME_PROPERTY),
                        trackingUrl));
    }

    /**
     * Starts the Hazelcast master and closes the driver if startup fails.
     *
     * @param engineConfiguration prepared Engine/Hazelcast configuration
     * @param specification resolved application fields
     * @param id native YARN application ID
     * @param driver resource manager driver owned by the master
     * @return started Hazelcast master instance
     */
    private static HazelcastInstanceImpl startMaster(
            SeaTunnelConfig engineConfiguration,
            ApplicationSpecification specification,
            String id,
            ResourceManagerDriver<?> driver) {
        try {
            return SeaTunnelServerStarter.createHazelcastInstance(
                    engineConfiguration,
                    null,
                    new ResourceManagerFactory(DeployType.YARN, id, specification, driver));
        } catch (Exception failure) {
            try {
                driver.close();
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /**
     * Runs the application job and releases driver and master resources afterwards.
     *
     * @param specification resolved application fields
     * @param master started Hazelcast master instance
     * @param driver resource manager driver to close during cleanup
     */
    private static void runJob(
            ApplicationSpecification specification,
            HazelcastInstanceImpl master,
            ResourceManagerDriver<?> driver)
            throws Exception {
        SeaTunnelServer server =
                master.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        CountDownLatch stopped = new CountDownLatch(1);
        Thread shutdown = shutdownHook(stopped);
        try {
            Runtime.getRuntime().addShutdownHook(shutdown);
            new ApplicationJobRunner(server, specification).run();
        } finally {
            removeShutdownHook(shutdown);
            closeDriverIfNeeded(server, driver);
            try {
                master.shutdown();
            } finally {
                stopped.countDown();
            }
        }
    }

    /**
     * Creates the shutdown hook that interrupts the owning thread and waits for cleanup.
     *
     * @param stopped latch released when the owning thread finishes cleanup
     * @return JVM shutdown hook thread
     */
    private static Thread shutdownHook(CountDownLatch stopped) {
        Thread owner = Thread.currentThread();
        return new Thread(
                () -> {
                    owner.interrupt();
                    try {
                        stopped.await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                "seatunnel-yarn-application-shutdown");
    }

    /**
     * Closes the driver when the runtime has not already initialized the resource manager.
     *
     * @param server running master server
     * @param driver driver to close if cleanup ownership belongs to this process
     */
    private static void closeDriverIfNeeded(SeaTunnelServer server, ResourceManagerDriver<?> driver)
            throws Exception {
        try {
            if (server.getCoordinatorService().getInitializedResourceManager() == null) {
                driver.close();
            }
        } finally {
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
