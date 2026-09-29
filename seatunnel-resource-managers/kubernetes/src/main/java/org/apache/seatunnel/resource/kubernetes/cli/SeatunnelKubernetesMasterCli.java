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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigSyntax;

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
import org.apache.seatunnel.engine.server.resourcemanager.thirdparty.kubernetes.KubernetesResourceManagerFactory;
import org.apache.seatunnel.resource.kubernetes.KubernetesResourceManagerDriver;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClientFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Runs the Kubernetes application master and its single native job. */
public final class SeatunnelKubernetesMasterCli {
    private static final Logger LOG = LoggerFactory.getLogger(SeatunnelKubernetesMasterCli.class);

    /**
     * Runs one job and maps its final outcome to the Kubernetes Job process exit code.
     *
     * @param args application ID followed by the mounted specification path
     * @throws Exception if arguments, localized configuration or driver initialization fail
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected application id and specification path");
        }
        String id = args[0];
        ApplicationSpecification specification = ApplicationSpecification.read(Paths.get(args[1]));
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(specification);
        try {
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
                    .setPortAutoIncrement(false);
            String host = System.getenv("SEATUNNEL_APPLICATION_MASTER_HOST");
            if (host != null && !host.trim().isEmpty()) {
                engineConfiguration
                        .getHazelcastConfig()
                        .getNetworkConfig()
                        .setPublicAddress(
                                (host.contains(":") ? "[" + host + "]" : host)
                                        + ":"
                                        + specification.getOption(ApplicationOptions.MASTER_PORT));
            }

            KubernetesResourceManagerDriver driver =
                    new KubernetesResourceManagerDriver(
                            KubernetesClientFactory.create(specification.getOptions(), true),
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
                                new KubernetesResourceManagerFactory<>(id, specification, driver));
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
        } catch (Exception failure) {
            LOG.error("Kubernetes application {} failed", id, failure);
            System.exit(1);
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
            // Waiting for all workers to be ready
            resources.awaitWorkerRegistration();
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(specification.getName());
            execution =
                    new ApplicationJobExecutionEnvironment(
                                    jobConfig,
                                    ConfigFactory.parseString(
                                            specification.getJobConfig(),
                                            ConfigParseOptions.defaults()
                                                    .setSyntax(ConfigSyntax.JSON)),
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
