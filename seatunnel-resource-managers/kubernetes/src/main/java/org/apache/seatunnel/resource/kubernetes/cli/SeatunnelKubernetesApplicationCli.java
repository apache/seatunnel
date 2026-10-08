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
            runApplication(args);
        } catch (Exception failure) {
            LOG.error("Kubernetes application failed", failure);
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    /** Starts the master, runs its job and closes the master before returning. */
    private static void runApplication(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected application id and specification path");
        }
        String id = args[0];
        // args[1] is application.properties mounted from the application Secret, not the user's
        // HOCON input file. Decode once and pass the resolved objects to the runtime components.
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.read(Paths.get(args[1]));
        ApplicationSpecification specification = parameters.getSpecification();
        SeaTunnelConfig engineConfiguration = ConfigProvider.locateAndGetSeaTunnelConfig();
        String clusterName = SeatunnelApplicationConfig.clusterName(id);
        SeatunnelApplicationConfig.configure(
                engineConfiguration,
                clusterName,
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
                    .setPublicAddress(
                            (host.contains(":") ? "[" + host + "]" : host)
                                    + ":"
                                    + specification.getMasterPort());
        }

        KubernetesResourceManagerDriver driver =
                new KubernetesResourceManagerDriver(
                        KubernetesClientFactory.create(
                                Collections.singletonMap(
                                        KubernetesOptions.NAMESPACE.key(),
                                        parameters.getNamespace()),
                                true),
                        parameters,
                        id,
                        clusterName);
        HazelcastInstanceImpl master;
        try {
            master =
                    SeaTunnelServerStarter.createHazelcastInstance(
                            engineConfiguration,
                            null,
                            JarPathResolver.identity(),
                            new ResourceManagerFactory(
                                    DeployType.KUBERNETES, id, specification, driver));
        } catch (Exception failure) {
            try {
                driver.close();
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
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
                        "seatunnel-kubernetes-application-shutdown");
        try {
            Runtime.getRuntime().addShutdownHook(shutdown);
            new ApplicationJobRunner(server, specification).run();
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdown);
            } catch (IllegalStateException ignored) {
                // The VM is already executing this hook.
            }

            try {
                if (server.getCoordinatorService().getInitializedResourceManager() == null) {
                    driver.close();
                }
            } finally {
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                }
            }
            master.shutdown();
            stopped.countDown();
        }
    }
}
