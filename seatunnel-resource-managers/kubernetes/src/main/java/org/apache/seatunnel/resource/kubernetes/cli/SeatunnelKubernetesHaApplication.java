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
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.application.ApplicationJobRunner;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.resource.kubernetes.KubernetesResourceManagerDriver;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.HaApplicationState;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClientFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesHaClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.HaApplicationFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Dual native masters share the normal Application runner and resource manager. */
public final class SeatunnelKubernetesHaApplication {
    private SeatunnelKubernetesHaApplication() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && "master".equals(args[1])) {
            String encoded = System.getenv(KubernetesConstants.SPECIFICATION_ENV);
            KubernetesApplicationConfig parameters = KubernetesApplicationConfig.read(encoded);
            if (!parameters.isHaEnabled()) {
                throw new IllegalArgumentException("HA configuration required");
            }
            startMaster(args[0], encoded, parameters);
        } else if (args.length == 6 && "owner".equals(args[1])) {
            try (KubernetesHaClient api = api(args[2])) {
                observe(
                        api,
                        args[0],
                        System.getenv(HaApplicationFactory.OWNER_UID_ENV),
                        args[5],
                        Long.parseLong(args[3]),
                        Long.parseLong(args[4]));
            }
        } else {
            throw new IllegalArgumentException(
                    "Expected <id> master or <id> owner <namespace> <job-id> <startup-timeout-ms> <spec-digest>");
        }
    }

    private static KubernetesHaClient api(KubernetesApplicationConfig parameters)
            throws IOException {
        return api(parameters.getNamespace());
    }

    private static KubernetesHaClient api(String namespace) throws IOException {
        return KubernetesHaClient.require(
                KubernetesClientFactory.create(
                        Collections.singletonMap(KubernetesOptions.NAMESPACE.key(), namespace),
                        true));
    }

    private static void startMaster(
            String id, String encoded, KubernetesApplicationConfig parameters) throws Exception {
        ApplicationSpecification specification = parameters.getSpecification();
        String expectedUid = System.getenv(HaApplicationFactory.OWNER_UID_ENV);
        try (KubernetesHaClient initialApi = api(parameters)) {
            HaApplicationState.validated(
                    initialApi.getJob(id), expectedUid, encoded, specification.getJobId());
        }
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        SeatunnelApplicationConfig.configureHa(
                config,
                SeatunnelApplicationConfig.clusterName(id),
                HaApplicationFactory.masterAddresses(id, parameters),
                false,
                specification.getWorkerSpecification().getSlots(),
                parameters.getHaBackupCount());
        SeatunnelApplicationConfig.configureCheckpointRetention(config);
        config.getHazelcastConfig()
                .getNetworkConfig()
                .setPort(specification.getMasterPort())
                .setPortAutoIncrement(false);
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.enabled", "true");
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.policy", "GRACEFUL");
        HazelcastInstanceImpl member =
                SeaTunnelServerStarter.createHazelcastInstance(
                        config,
                        null,
                        ResourceManagerFactory.forApplication(
                                id,
                                specification,
                                () -> {
                                    try {
                                        return new KubernetesResourceManagerDriver(
                                                api(parameters),
                                                parameters,
                                                id,
                                                SeatunnelApplicationConfig.clusterName(id),
                                                expectedUid);
                                    } catch (IOException e) {
                                        throw new UncheckedIOException(e);
                                    }
                                }));
        SeaTunnelServer server =
                member.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        try {
            while (member.getLifecycleService().isRunning()) {
                if (!server.isMasterNode()) {
                    TimeUnit.MILLISECONDS.sleep(100);
                    continue;
                }
                CoordinatorService coordinator =
                        server.awaitCoordinatorService(specification.getStartupTimeoutMillis());
                final var activationResources = coordinator.getResourceManager();
                try (KubernetesHaClient api = api(parameters)) {
                    KubernetesJob owner = api.getJob(id);
                    String uid = expectedUid;
                    ApplicationJobRunner.Lifecycle lifecycle =
                            new ApplicationJobRunner.Lifecycle() {
                                private KubernetesJob current() {
                                    return HaApplicationState.validated(
                                            api.getJob(id), uid, encoded, specification.getJobId());
                                }

                                @Override
                                public boolean isCurrent() {
                                    return member.getLifecycleService().isRunning()
                                            && server.isMasterNode()
                                            && coordinator.isCoordinatorActive()
                                            && coordinator.getInitializedResourceManager()
                                                    == activationResources;
                                }

                                @Override
                                public void awaitReady() throws Exception {
                                    long deadline =
                                            System.nanoTime()
                                                    + TimeUnit.MILLISECONDS.toNanos(
                                                            specification
                                                                    .getStartupTimeoutMillis());
                                    while (HaApplicationState.unsubmitted(current())
                                            && member.getCluster().getMembers().stream()
                                                            .filter(m -> !m.isLiteMember())
                                                            .count()
                                                    < 2) {
                                        if (!isCurrent()) {
                                            return;
                                        }
                                        if (System.nanoTime() > deadline) {
                                            throw new TimeoutException(
                                                    "Second native master did not join");
                                        }
                                        TimeUnit.MILLISECONDS.sleep(100);
                                    }
                                }

                                @Override
                                public boolean submit() {
                                    return HaApplicationState.claimSubmission(api, current());
                                }

                                @Override
                                public void running() {
                                    HaApplicationState.recordRunning(api, current());
                                }

                                @Override
                                public boolean hasStarted() {
                                    return HaApplicationState.status(current())
                                            == ApplicationStatus.RUNNING;
                                }

                                @Override
                                public void finished(ApplicationStatus status) {
                                    if (isCurrent()) {
                                        HaApplicationState.recordTerminal(
                                                api, current(), status, this::isCurrent);
                                    }
                                }
                            };
                    if (HaApplicationState.terminal(
                                    HaApplicationState.validated(
                                            owner, uid, encoded, specification.getJobId()))
                            != null) {
                        return;
                    }
                    try {
                        new ApplicationJobRunner(server, specification, lifecycle).run();
                    } catch (Exception failure) {
                        if (lifecycle.isCurrent()) {
                            throw failure;
                        }
                    }
                    // Native coordinator demotion detaches the local driver without deleting
                    // workers.
                }
                TimeUnit.MILLISECONDS.sleep(100);
            }
        } finally {
            member.shutdown();
        }
    }

    /** Retryable metadata observer; never submits or recovers a pipeline. */
    static void observe(
            KubernetesHaClient api,
            String id,
            String uid,
            String expectedDigest,
            long jobId,
            long startupTimeoutMillis)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(startupTimeoutMillis);
        KubernetesJob owner;
        while (true) {
            owner = HaApplicationState.validatedDigest(api.getJob(id), uid, expectedDigest, jobId);
            if (HaApplicationState.terminal(owner) != null) {
                break;
            }
            if (System.nanoTime() > deadline
                    && HaApplicationState.recordStartupFailure(api, owner)) {
                owner =
                        HaApplicationState.validatedDigest(
                                api.getJob(id), uid, expectedDigest, jobId);
                break;
            }
            TimeUnit.SECONDS.sleep(1);
        }
        HaApplicationState.finalizeResources(api, owner);
    }
}
