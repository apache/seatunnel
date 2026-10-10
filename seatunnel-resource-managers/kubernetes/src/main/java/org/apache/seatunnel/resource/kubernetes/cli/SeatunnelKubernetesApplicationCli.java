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

package org.apache.seatunnel.resource.kubernetes.cli;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.core.classloader.JarPathResolver;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.application.ApplicationJobRunner;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.resource.kubernetes.KubernetesResourceManagerDriver;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClientFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Runs the Kubernetes application master and its single native job. */
public final class SeatunnelKubernetesApplicationCli {
    private static final Logger LOG =
            LoggerFactory.getLogger(SeatunnelKubernetesApplicationCli.class);

    /**
     * Runs one job and maps its final outcome to the Kubernetes Job process exit code.
     *
     * @param args application ID followed by the mounted specification path
     */
    public static void main(String[] args) {
        int exitCode = 0;
        try {
            String id = applicationId(args);
            LOG.info("Running Kubernetes application {}", id);
            KubernetesApplicationParameters parameters =
                    KubernetesApplicationParameters.read(Paths.get(args[1]));
            ApplicationSpecification specification = parameters.getSpecification();
            LOG.debug("Loaded application specification {}", specification.getName());
            SeaTunnelConfig engineConfiguration = configureEngine(id, specification);
            KubernetesResourceManagerDriver driver = createDriver(parameters, id, clusterName(id));
            HazelcastInstanceImpl master =
                    startMaster(engineConfiguration, specification, id, driver);
            LOG.info("Master started for Kubernetes application {}", id);
            runJob(specification, master, driver);
        } catch (Exception failure) {
            LOG.error("Kubernetes application failed", failure);
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    /**
     * Validates the Kubernetes master entrypoint arguments.
     *
     * @param args application ID followed by the mounted specification path
     * @return the Kubernetes application ID
     */
    private static String applicationId(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected application id and specification path");
        }
        return args[0];
    }

    /**
     * Builds the isolated Hazelcast cluster name for one application.
     *
     * @param id Kubernetes application ID
     * @return cluster name shared only by this application's master and workers
     */
    private static String clusterName(String id) {
        return SeatunnelApplicationConfig.clusterName(id);
    }

    /**
     * Prepares the master's Engine and Hazelcast configuration.
     *
     * @param id Kubernetes application ID used for the cluster name
     * @param specification resolved application fields
     * @return configuration ready for master startup
     */
    private static SeaTunnelConfig configureEngine(
            String id, ApplicationSpecification specification) {
        SeaTunnelConfig engineConfiguration = ConfigProvider.locateAndGetSeaTunnelConfig();
        SeatunnelApplicationConfig.configure(
                engineConfiguration,
                clusterName(id),
                null,
                specification.getWorkerSpecification().getSlots());
        SeatunnelApplicationConfig.configureCheckpointRetention(engineConfiguration);
        engineConfiguration
                .getHazelcastConfig()
                .getNetworkConfig()
                .setPort(specification.getMasterPort())
                .setPortAutoIncrement(false);
        String host = System.getenv(KubernetesConstants.MASTER_HOST_ENV);
        if (host != null && !host.trim().isEmpty()) {
            engineConfiguration
                    .getHazelcastConfig()
                    .getNetworkConfig()
                    .setPublicAddress(masterAddress(host, specification.getMasterPort()));
        }
        return engineConfiguration;
    }

    /**
     * Opens the in-cluster Kubernetes client and creates the resource manager driver.
     *
     * @param parameters resolved Kubernetes settings
     * @param id Kubernetes application ID
     * @param clusterName isolated Hazelcast cluster name
     * @return driver that owns the in-cluster SDK connection
     */
    private static KubernetesResourceManagerDriver createDriver(
            KubernetesApplicationParameters parameters, String id, String clusterName)
            throws Exception {
        return new KubernetesResourceManagerDriver(
                KubernetesClientFactory.create(
                        Collections.singletonMap(
                                KubernetesOptions.NAMESPACE.key(), parameters.getNamespace()),
                        true),
                parameters,
                id,
                clusterName);
    }

    /**
     * Starts the Hazelcast master and closes the driver if startup fails.
     *
     * @param engineConfiguration prepared Engine/Hazelcast configuration
     * @param specification resolved application fields
     * @param id Kubernetes application ID
     * @param driver resource manager driver owned by the master
     * @return started Hazelcast master instance
     */
    private static HazelcastInstanceImpl startMaster(
            SeaTunnelConfig engineConfiguration,
            ApplicationSpecification specification,
            String id,
            KubernetesResourceManagerDriver driver) {
        try {
            return SeaTunnelServerStarter.createHazelcastInstance(
                    engineConfiguration,
                    null,
                    JarPathResolver.identity(),
                    new ResourceManagerFactory(DeployType.KUBERNETES, id, specification, driver));
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
            KubernetesResourceManagerDriver driver)
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
                        stopped.await(
                                KubernetesConstants.TERMINATION_GRACE_PERIOD_SECONDS,
                                TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                "seatunnel-kubernetes-application-shutdown");
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
            // The VM is already executing this hook.
        }
    }

    /**
     * Closes the driver when the runtime has not already initialized the resource manager.
     *
     * @param server running master server
     * @param driver driver to close if cleanup ownership belongs to this process
     */
    private static void closeDriverIfNeeded(
            SeaTunnelServer server, KubernetesResourceManagerDriver driver) throws Exception {
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

    /**
     * Formats a master endpoint with IPv6 brackets when required.
     *
     * @param host master host or IP address
     * @param port master port
     * @return host:port endpoint suitable for Hazelcast public address configuration
     */
    private static String masterAddress(String host, int port) {
        String formattedHost = host.contains(":") ? "[" + host + "]" : host;
        return formattedHost + ":" + port;
    }
}
