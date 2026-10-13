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

package org.apache.seatunnel.resource.e2e.kubernetes;

import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationClient;
import org.apache.seatunnel.resource.kubernetes.worker.SeatunnelKubernetesApplicationWorker;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1ResourceQuota;
import io.kubernetes.client.util.Yaml;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies isolation, cancellation, worker loss and quota cleanup in a disposable K3s. */
public class KubernetesApplicationLifecycleIT extends AbstractKubernetesApplicationIT {
    private static final String CONTAINERD_RUNTIME_PREFIX = "containerd://";
    private static final long QUOTA_READY_TIMEOUT_SECONDS = 30;

    @Test
    void simultaneousApplicationsAreIsolatedAndCancellationRemovesEverything() throws Exception {
        ApplicationSpecification firstSpecification = specification("isolation");
        try (KubernetesApplicationClient first =
                deployApplication(firstSpecification, "isolation")) {
            try (KubernetesApplicationClient second = deployApplication("isolation")) {
                try {
                    awaitWorkersRunning(first, 1);
                    awaitWorkersRunning(second, 1);
                    awaitJobRunning(first);
                    awaitJobRunning(second);
                    // Pod addresses are cluster-internal; retrieving a provider must not connect.
                    assertNotNull(deployer.retrieve(first.getClusterId()));
                    assertNotNull(deployer.retrieve(second.getClusterId()));
                    assertNotEquals(first.getClusterId(), second.getClusterId());
                    String firstMaster = masterPod(first).getStatus().getPodIP() + ":5801";
                    String secondMaster = masterPod(second).getStatus().getPodIP() + ":5801";
                    assertNotEquals(firstMaster, secondMaster);
                    for (V1Pod worker : workers(first)) {
                        assertRuntimeConfigMapMounted(worker);
                        List<String> command = worker.getSpec().getContainers().get(0).getCommand();
                        assertTrue(
                                command.contains(
                                        SeatunnelKubernetesApplicationWorker.class.getName()));
                        int entrypoint =
                                command.indexOf(
                                        SeatunnelKubernetesApplicationWorker.class.getName());
                        assertEquals(
                                SeatunnelApplicationConfig.clusterName(first.getClusterId()),
                                command.get(entrypoint + 1));
                        assertEquals(firstMaster, command.get(entrypoint + 2));
                        assertEquals(
                                String.valueOf(
                                        firstSpecification.getWorkerSpecification().getSlots()),
                                command.get(entrypoint + 3));
                        assertTrue(command.contains("-Dhazelcast.logging.type=log4j2"));
                        assertTrue(
                                command.stream()
                                        .anyMatch(
                                                argument ->
                                                        argument.startsWith(
                                                                "-Dlog4j2.configurationFile=")));
                        assertTrue(
                                command.stream()
                                        .anyMatch(
                                                argument ->
                                                        argument.startsWith(
                                                                "-Dseatunnel.logs.path=")));
                        assertTrue(command.contains(firstMaster));
                        assertFalse(command.contains(secondMaster));
                        assertEquals(
                                first.getClusterId(),
                                worker.getMetadata().getOwnerReferences().get(0).getName());
                    }
                    assertRuntimeConfigMapMounted(masterPod(first));
                    assertRuntimeConfigMapMounted(masterPod(second));
                    cancelAndAwait(first);
                    assertEquals(
                            ApplicationStatus.UNKNOWN,
                            deployer.getApplicationStatus(first.getClusterId()));
                    assertEquals(
                            ApplicationStatus.RUNNING,
                            deployer.getApplicationStatus(second.getClusterId()));
                    assertEquals(1, workers(second).size());
                } finally {
                    cancelAndAwait(second);
                }
            } finally {
                cancelAndAwait(first);
            }
        }
    }

    @Test
    void workerLossFailsApplicationAndCleansRemainingWorkers() throws Exception {
        try (KubernetesApplicationClient application = deployApplication("worker-loss")) {
            try {
                awaitWorkersRunning(application, 2);
                awaitJobRunning(application);
                core.deleteNamespacedPod(
                        workers(application).get(0).getMetadata().getName(),
                        namespace,
                        null,
                        null,
                        0,
                        null,
                        "Background",
                        null);
                awaitStatus(application, ApplicationStatus.FAILED);
                awaitWorkersRemoved(application);
                assertEquals(
                        ApplicationStatus.FAILED,
                        application.getStatus(),
                        "Worker cleanup must preserve the original application failure");
            } finally {
                cancelAndAwait(application);
            }
        }
    }

    @Test
    void masterExitTerminatesWorkersWithoutMasterCleanup() throws Exception {
        try (KubernetesApplicationClient application = deployApplication("isolation")) {
            try {
                awaitWorkersRunning(application, 1);
                awaitJobRunning(application);
                V1Pod master = masterPod(application);
                killPodContainer(master);
                awaitStatus(application, ApplicationStatus.FAILED);
                awaitWorkersExited(application);
            } finally {
                cancelAndAwait(application);
            }
        }
    }

    @Test
    void quotaFailureDuringWorkerAllocationCleansPartialApplication() throws Exception {
        V1ResourceQuota workerQuota =
                Yaml.loadAs(suiteFile("worker-quota.yaml"), V1ResourceQuota.class);
        String quota = workerQuota.getMetadata().getName();
        core.createNamespacedResourceQuota(namespace, workerQuota, null, null, null, null);
        try {
            Awaitility.await()
                    .atMost(QUOTA_READY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                V1ResourceQuota current =
                                        core.readNamespacedResourceQuota(quota, namespace, null);
                                return current.getStatus() != null
                                        && current.getStatus().getHard() != null;
                            });
            try (KubernetesApplicationClient application =
                    deployApplication(specification("quota"), "quota")) {
                try {
                    awaitStatus(application, ApplicationStatus.FAILED);
                    awaitWorkersRemoved(application);
                } finally {
                    cancelAndAwait(application);
                }
            }
        } finally {
            core.deleteNamespacedResourceQuota(
                    quota, namespace, null, null, 0, null, "Background", null);
        }
    }

    private static void awaitJobRunning(KubernetesApplicationClient application) {
        withPodDiagnostics(
                application,
                () ->
                        Awaitility.await()
                                .atMost(STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                                .pollInterval(2, TimeUnit.SECONDS)
                                .untilAsserted(
                                        () ->
                                                assertTrue(
                                                        livePodLogs(
                                                                        application,
                                                                        masterPod(application)
                                                                                .getMetadata()
                                                                                .getName())
                                                                .contains(
                                                                        "turned from state SCHEDULED to RUNNING."))));
    }

    private static void awaitWorkersExited(KubernetesApplicationClient application) {
        Awaitility.await()
                .atMost(CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<V1Pod> workers = workers(application);
                            assertFalse(
                                    workers.isEmpty(),
                                    "The application must launch at least one worker");
                            assertTrue(
                                    workers.stream()
                                            .allMatch(
                                                    pod ->
                                                            pod.getStatus() != null
                                                                    && ("Succeeded"
                                                                                    .equals(
                                                                                            pod.getStatus()
                                                                                                    .getPhase())
                                                                            || "Failed"
                                                                                    .equals(
                                                                                            pod.getStatus()
                                                                                                    .getPhase()))));
                        });
    }

    private static void assertRuntimeConfigMapMounted(V1Pod pod) {
        assertTrue(
                pod.getSpec().getVolumes().stream()
                        .anyMatch(
                                volume ->
                                        volume.getConfigMap() != null
                                                && RUNTIME_CONFIG_MAP.equals(
                                                        volume.getConfigMap().getName())));
        assertTrue(mountedReadOnly(pod, "/opt/seatunnel/config/seatunnel.yaml"));
        assertTrue(mountedReadOnly(pod, "/opt/seatunnel/config/log4j2_client.properties"));
    }

    /** Kills the container of one application Pod inside the disposable cluster. */
    private static void killPodContainer(V1Pod pod) throws Exception {
        String containerId = pod.getStatus().getContainerStatuses().get(0).getContainerID();
        Container.ExecResult result =
                execInCluster(
                        "ctr",
                        "--address",
                        CONTAINERD_SOCKET,
                        "--namespace",
                        "k8s.io",
                        "tasks",
                        "kill",
                        "--signal",
                        "SIGKILL",
                        containerId.substring(CONTAINERD_RUNTIME_PREFIX.length()));
        assertEquals(0, result.getExitCode(), result.getStderr());
    }
}
