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
        System.exit(runApplication());
    }

    /**
     * Loads the localized configuration, starts the master, and runs the application job. It
     * registers a shutdown hook for staging cleanup and attempts the same cleanup on exit, even if
     * startup or job execution fails. Any escaping {@link Throwable} is caught so {@code main} can
     * report fatal errors to YARN through the process exit code.
     *
     * @return 0 on normal completion or 1 for an unhandled exception or error
     */
    static int runApplication() {
        try {
            Configuration configuration =
                    YarnConfigurationUtils.loadLocalized(
                            YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME);
            Path staging = YarnStagingDirectory.fromEnvironment();
            // SIGTERM runs JVM hooks. A forced kill can leave staging for client-side cleanup.
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
                SeaTunnelConfig engineConfiguration = YarnConfigurationUtils.loadEngineConfig();
                configureEngine(
                        engineConfiguration,
                        id,
                        specification,
                        applicationConfiguration.getMasterHttpPort());
                ResourceManagerDriver<?> driver =
                        createDriver(
                                configuration,
                                staging,
                                applicationConfiguration,
                                clusterName(id),
                                trackingUrl(
                                        engineConfiguration.getEngineConfig().getHttpConfig(),
                                        System.getenv(
                                                ApplicationConstants.Environment.NM_HOST.name())));
                HazelcastInstanceImpl master =
                        startMaster(engineConfiguration, specification, id, driver);
                LOG.info("Master started for YARN application {}", id);
                runJob(specification, master, driver);
            } finally {
                LOG.info("Removing YARN application staging directory {}", staging);
                cleanupStaging(configuration, staging);
                removeShutdownHook(cleanup);
            }
            return 0;
        } catch (Throwable failure) {
            LOG.error("YARN application failed", failure);
            return 1;
        }
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
     * @param engineConfiguration localized engine configuration to prepare for master startup
     * @param id native YARN application ID used for the cluster name
     * @param specification resolved application fields
     * @param masterHttpPort optional fixed port for the AM web endpoint
     */
    static void configureEngine(
            SeaTunnelConfig engineConfiguration,
            String id,
            ApplicationSpecification specification,
            Integer masterHttpPort) {
        HttpConfig http = engineConfiguration.getEngineConfig().getHttpConfig();
        SeatunnelApplicationConfig.configure(
                engineConfiguration,
                clusterName(id),
                null,
                specification.getWorkerSpecification().getSlots());
        if (masterHttpPort != null) {
            // The common application setup disables web listeners. Keep the user's HTTP auth,
            // context path and request limits when enabling the AM's fixed endpoint.
            if (http == ServerConfigOptions.MasterServerConfigOptions.HTTP.defaultValue()) {
                http = new HttpConfig();
            }
            // Jetty creates independent HTTP and HTTPS connectors. Bind only the selected one.
            http.setEnabled(!http.isEnableHttps());
            http.setEnableDynamicPort(false);
            if (http.isEnableHttps()) {
                http.setHttpsPort(masterHttpPort);
            } else {
                http.setPort(masterHttpPort);
            }
            engineConfiguration.getEngineConfig().setHttpConfig(http);
        }
        SeatunnelApplicationConfig.configureCheckpointRetention(engineConfiguration);
        engineConfiguration
                .getHazelcastConfig()
                .getNetworkConfig()
                .setPort(specification.getMasterPort())
                .setPortAutoIncrement(true);
    }

    /** Builds the AM's original tracking URL on its NodeManager host for the YARN web proxy. */
    static String trackingUrl(HttpConfig http, String host) throws URISyntaxException {
        if (!http.isEnabled() && !http.isEnableHttps()) {
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
                        http.isEnableHttps() ? "https" : "http",
                        null,
                        host,
                        http.isEnableHttps() ? http.getHttpsPort() : http.getPort(),
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
    static HazelcastInstanceImpl startMaster(
            SeaTunnelConfig engineConfiguration,
            ApplicationSpecification specification,
            String id,
            ResourceManagerDriver<?> driver) {
        try {
            return SeaTunnelServerStarter.createHazelcastInstance(
                    engineConfiguration,
                    null,
                    new ResourceManagerFactory(DeployType.YARN, id, specification, driver));
        } catch (Throwable failure) {
            closeDriverAfterFailure(driver, failure);
            throw failure;
        }
    }

    private static void closeDriverAfterFailure(
            ResourceManagerDriver<?> driver, Throwable failure) {
        try {
            driver.close();
        } catch (Throwable cleanup) {
            failure.addSuppressed(cleanup);
        }
    }

    /**
     * Runs the application job and releases driver and master resources afterwards.
     *
     * @param specification resolved application fields
     * @param master started Hazelcast master instance
     * @param driver resource manager driver to close during cleanup
     */
    static void runJob(
            ApplicationSpecification specification,
            HazelcastInstanceImpl master,
            ResourceManagerDriver<?> driver)
            throws Exception {
        CountDownLatch stopped = new CountDownLatch(1);
        Thread shutdown = shutdownHook(stopped);
        try (AutoCloseable closeMaster = master::shutdown;
                AutoCloseable removeHook = () -> removeShutdownHook(shutdown)) {
            SeaTunnelServer server = null;
            try {
                Runtime.getRuntime().addShutdownHook(shutdown);
                server = master.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
                new ApplicationJobRunner(server, specification).run();
            } catch (Throwable failure) {
                // Before resource-manager initialization there is no runtime owner to close the
                // driver. Checking ownership must also work when the coordinator never activated.
                if (server == null || !server.hasInitializedResourceManager()) {
                    closeDriverAfterFailure(driver, failure);
                }
                throw failure;
            }
        } finally {
            stopped.countDown();
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
}
