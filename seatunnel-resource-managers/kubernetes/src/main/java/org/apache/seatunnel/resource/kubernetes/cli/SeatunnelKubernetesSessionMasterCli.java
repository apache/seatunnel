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
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.resource.kubernetes.KubernetesSessionResourceManagerDriver;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesSessionConfig;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClientFactory;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

/** Starts one Kubernetes session master and keeps it alive until the process is stopped. */
@Slf4j
public final class SeatunnelKubernetesSessionMasterCli {

    private static final String SESSION_CONFIG_ENV = "SEATUNNEL_SESSION_CONFIG";
    private static final String DEFAULT_SESSION_CONFIG = "/opt/seatunnel/config/session.yaml";
    private static final String MASTER_HOST_ENV = "SEATUNNEL_SESSION_MASTER_HOST";

    private SeatunnelKubernetesSessionMasterCli() {}

    public static void main(String[] args) {
        try {
            new SeatunnelKubernetesSessionMasterCli().run(args);
        } catch (Exception failure) {
            log.error("Kubernetes session master failed", failure);
            System.exit(1);
        }
    }

    private void run(String[] args) throws Exception {
        KubernetesClient client = null;
        HazelcastInstanceImpl master = null;
        try {
            Path path = sessionConfigPath(args);
            KubernetesSessionConfig config = KubernetesSessionConfig.read(path);
            client =
                    KubernetesClientFactory.create(
                            Collections.singletonMap(
                                    KubernetesOptions.NAMESPACE.key(), config.getNamespace()),
                            true);
            SeaTunnelConfig engineConfiguration = configureEngine(config);
            validateFixedSlots(config, engineConfiguration);
            final KubernetesClient sessionClient = client;
            master =
                    SeaTunnelServerStarter.createMasterHazelcastInstance(
                            engineConfiguration,
                            new ResourceManagerFactory(
                                    config.getSessionSpecification(),
                                    () ->
                                            new KubernetesSessionResourceManagerDriver(
                                                    sessionClient,
                                                    config,
                                                    config.getSessionSpecification().getName())));
            log.info(
                    "Kubernetes session master started for {}",
                    config.getSessionSpecification().getName());
            awaitShutdown(master);
        } finally {
            closeQuietly(master, client);
        }
    }

    private static Path sessionConfigPath(String[] args) {
        if (args.length > 0 && args[0] != null && !args[0].trim().isEmpty()) {
            return Paths.get(args[0]);
        }
        String property = System.getProperty("seatunnel.session.config");
        if (property != null && !property.trim().isEmpty()) {
            return Paths.get(property);
        }
        String environment = System.getenv(SESSION_CONFIG_ENV);
        return Paths.get(
                environment == null || environment.trim().isEmpty()
                        ? DEFAULT_SESSION_CONFIG
                        : environment);
    }

    /**
     * Applies the session cluster name, member port and pod public address to the shared engine
     * config.
     */
    private static SeaTunnelConfig configureEngine(KubernetesSessionConfig config) {
        SeaTunnelConfig engineConfiguration = ConfigProvider.locateAndGetSeaTunnelConfig();
        engineConfiguration
                .getHazelcastConfig()
                .setClusterName(config.getSessionSpecification().getClusterName());
        engineConfiguration
                .getHazelcastConfig()
                .getNetworkConfig()
                .setPort(config.getMasterPort())
                .setPortAutoIncrement(false);
        String host = System.getenv(MASTER_HOST_ENV);
        if (host != null && !host.trim().isEmpty()) {
            engineConfiguration
                    .getHazelcastConfig()
                    .getNetworkConfig()
                    .setPublicAddress(masterAddress(host.trim(), config.getMasterPort()));
        }
        return engineConfiguration;
    }

    private static void awaitShutdown(HazelcastInstanceImpl master) throws InterruptedException {
        CountDownLatch stopped = new CountDownLatch(1);
        Thread shutdown =
                new Thread(
                        () -> {
                            try {
                                master.shutdown();
                            } finally {
                                stopped.countDown();
                            }
                        },
                        "seatunnel-kubernetes-session-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdown);
        try {
            stopped.await();
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdown);
            } catch (IllegalStateException ignored) {
                // The VM is already executing the hook.
            }
        }
    }

    /** Checks the fixed-slot contract against the shared engine configuration. */
    private static void validateFixedSlots(
            KubernetesSessionConfig config, SeaTunnelConfig engineConfiguration) {
        SlotServiceConfig slotService =
                engineConfiguration.getEngineConfig().getSlotServiceConfig();
        if (slotService.isDynamicSlot()) {
            throw new IllegalArgumentException(
                    "Session Cluster supports fixed slots only; set "
                            + "seatunnel.engine.slot-service.dynamic-slot=false");
        }
        int configuredSlots = config.getSessionSpecification().getWorkerSpecification().getSlots();
        if (slotService.getSlotNum() != configuredSlots) {
            throw new IllegalArgumentException(
                    String.format(
                            "session.worker.slots=%d must equal seatunnel.yaml slot-num=%d",
                            configuredSlots, slotService.getSlotNum()));
        }
    }

    private static void closeQuietly(HazelcastInstanceImpl master, KubernetesClient client) {
        try {
            if (master != null) {
                master.shutdown();
            }
        } catch (Exception failure) {
            log.warn("Failed to stop session master", failure);
        }
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception failure) {
            log.warn("Failed to close Kubernetes client", failure);
        }
    }

    private static String masterAddress(String host, int port) {
        return (host.contains(":") ? "[" + host + "]" : host) + ":" + port;
    }
}
