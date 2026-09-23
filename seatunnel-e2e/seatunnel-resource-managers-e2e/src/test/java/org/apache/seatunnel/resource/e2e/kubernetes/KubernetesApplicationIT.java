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

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.client.ApplicationClient;
import org.apache.seatunnel.resource.core.client.ApplicationDeployer;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationDeployerFactory;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;

import org.codehaus.plexus.util.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import io.kubernetes.client.Exec;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimSpec;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PolicyRule;
import io.kubernetes.client.openapi.models.V1ResourceQuota;
import io.kubernetes.client.openapi.models.V1ResourceQuotaSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Role;
import io.kubernetes.client.openapi.models.V1RoleBinding;
import io.kubernetes.client.openapi.models.V1RoleRef;
import io.kubernetes.client.openapi.models.V1ServiceAccount;
import io.kubernetes.client.openapi.models.V1Subject;
import io.kubernetes.client.util.Config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.getResourcesFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs real application processes in a disposable K3s cluster using Maven test dependencies. */
@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK, EngineType.SPARK, EngineType.SEATUNNEL})
public class KubernetesApplicationIT extends TestSuiteBase {
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesApplicationIT.class);
    private static final String APPLICATION_LABEL = "seatunnel.apache.org/application-id";
    private static final String ROLE_LABEL = "seatunnel.apache.org/role";
    private final String namespace =
            "seatunnel-app-it-" + UUID.randomUUID().toString().substring(0, 8);
    private CoreV1Api core;
    private BatchV1Api batch;
    private ApplicationDeployer deployer;
    private Map<String, String> options;
    private boolean namespaceCreated;
    private ApiClient apiClient;
    private K3sContainer k3s;
    private Path kubeconfig;

    @BeforeAll
    void prepareApplicationEnvironment() throws Exception {
        String image = System.getProperty("seatunnel.kubernetes.test.image");
        if (image == null) {
            image = buildApplicationImage();
        }
        k3s =
                new K3sContainer(DockerImageName.parse("rancher/k3s:v1.31.6-k3s1"))
                        .withStartupTimeout(Duration.ofMinutes(3));
        k3s.start();
        kubeconfig = Files.createTempFile("seatunnel-k3s-", ".yaml");
        Files.write(kubeconfig, k3s.getKubeConfigYaml().getBytes(StandardCharsets.UTF_8));
        importImage(image);
        // These short tests need four schedulable CPUs for one master and three workers. Remove
        // only CPU reservations from system deployments in this disposable cluster; actual CPU
        // use remains shared, memory reservations and application quotas are unchanged.
        Container.ExecResult systemCpuRequests =
                k3s.execInContainer(
                        "kubectl",
                        "set",
                        "resources",
                        "deployment",
                        "--all",
                        "--namespace=kube-system",
                        "--requests=cpu=0");
        assertEquals(0, systemCpuRequests.getExitCode(), systemCpuRequests.getStderr());
        assertTrue(systemCpuRequests.getStdout().contains("coredns"));
        apiClient = Config.fromConfig(kubeconfig.toString());
        apiClient.setReadTimeout(30000);
        core = new CoreV1Api(apiClient);
        batch = new BatchV1Api(apiClient);
        // Unavailable clusters are failures rather than silently skipped integration coverage.
        core.listNamespace(null, null, null, null, null, null, null, null, null, 10, null);
        core.createNamespace(
                new V1Namespace().metadata(new V1ObjectMeta().name(namespace)),
                null,
                null,
                null,
                null);
        namespaceCreated = true;
        core.createNamespacedServiceAccount(
                namespace,
                new V1ServiceAccount().metadata(new V1ObjectMeta().name("application")),
                null,
                null,
                null,
                null);
        RbacAuthorizationV1Api rbac = new RbacAuthorizationV1Api(apiClient);
        rbac.createNamespacedRole(
                namespace,
                new V1Role()
                        .metadata(new V1ObjectMeta().name("application"))
                        .addRulesItem(
                                new V1PolicyRule()
                                        .apiGroups(Collections.singletonList(""))
                                        .resources(Collections.singletonList("pods"))
                                        .verbs(
                                                Arrays.asList(
                                                        "create",
                                                        "get",
                                                        "list",
                                                        "delete",
                                                        "deletecollection")))
                        .addRulesItem(
                                new V1PolicyRule()
                                        .apiGroups(Collections.singletonList("batch"))
                                        .resources(Collections.singletonList("jobs"))
                                        .verbs(Collections.singletonList("get"))),
                null,
                null,
                null,
                null);
        rbac.createNamespacedRoleBinding(
                namespace,
                new V1RoleBinding()
                        .metadata(new V1ObjectMeta().name("application"))
                        .roleRef(
                                new V1RoleRef()
                                        .apiGroup("rbac.authorization.k8s.io")
                                        .kind("Role")
                                        .name("application"))
                        .addSubjectsItem(
                                new V1Subject()
                                        .kind("ServiceAccount")
                                        .name("application")
                                        .namespace(namespace)),
                null,
                null,
                null,
                null);
        options = new HashMap<>();
        options.put(KubernetesOptions.NAMESPACE.key(), namespace);
        options.put(KubernetesOptions.KUBE_CONFIG.key(), kubeconfig.toString());
        options.put(KubernetesOptions.IMAGE.key(), image);
        options.put(KubernetesOptions.IMAGE_PULL_POLICY.key(), "Never");
        options.put(KubernetesOptions.SERVICE_ACCOUNT.key(), "application");
        options.put(ApplicationOptions.WORKER_COUNT.key(), "2");
        options.put(ApplicationOptions.WORKER_MEMORY_MB.key(), "768");
        options.put(ApplicationOptions.MASTER_MEMORY_MB.key(), "768");
        options.put(ApplicationOptions.STARTUP_TIMEOUT_MILLIS.key(), "180000");
        deployer = new KubernetesApplicationDeployerFactory().create(options);
    }

    @AfterAll
    void removeApplicationEnvironment() throws Exception {
        try {
            if (deployer != null) {
                deployer.close();
            }
        } finally {
            try {
                if (namespaceCreated) {
                    core.deleteNamespace(namespace, null, null, 0, null, "Foreground", null);
                }
            } finally {
                if (apiClient != null) {
                    apiClient.getHttpClient().dispatcher().executorService().shutdown();
                    apiClient.getHttpClient().connectionPool().evictAll();
                }
                if (k3s != null) {
                    k3s.close();
                }
                if (kubeconfig != null) {
                    Files.deleteIfExists(kubeconfig);
                }
            }
        }
    }

    @Test
    void fakeSourceSubmissionCompletesWithAssertAndReleasesWorkers() throws Exception {
        try (ApplicationClient application =
                deployer.deploy(specification(assertSubmissionJob()))) {
            try {
                awaitStatus(application, ApplicationStatus.SUCCEEDED);
                awaitWorkersRemoved(application);
                assertEquals(
                        1,
                        batch.readNamespacedJob(
                                        application.getApplicationId().getId(), namespace, null)
                                .getStatus()
                                .getSucceeded());
                assertEquals(
                        1,
                        core.readNamespacedConfigMap(
                                        application.getApplicationId().getId(), namespace, null)
                                .getMetadata()
                                .getOwnerReferences()
                                .size());
            } finally {
                application.cancel();
            }
            awaitAllResourcesRemoved(application);
        }
    }

    @Test
    void invalidJobFailsAndReleasesWorkers() throws Exception {
        try (ApplicationClient application =
                deployer.deploy(specification(job("BATCH", "ConnectorThatDoesNotExist")))) {
            try {
                awaitStatus(application, ApplicationStatus.FAILED);
                awaitWorkersRemoved(application);
            } finally {
                application.cancel();
            }
            awaitAllResourcesRemoved(application);
        }
    }

    @Test
    void simultaneousApplicationsAreIsolatedAndCancellationRemovesEverything() throws Exception {
        try (ApplicationClient first =
                deployer.deploy(singleWorkerSpecification(job("STREAMING", "Console", 1)))) {
            try (ApplicationClient second =
                    deployer.deploy(singleWorkerSpecification(job("STREAMING", "Console", 1)))) {
                try {
                    awaitWorkersRunning(first, 1);
                    awaitWorkersRunning(second, 1);
                    awaitJobRunning(first);
                    awaitJobRunning(second);
                    assertNotEquals(
                            first.getApplicationId().getId(), second.getApplicationId().getId());
                    String firstMaster = masterPod(first).getStatus().getPodIP() + ":5801";
                    String secondMaster = masterPod(second).getStatus().getPodIP() + ":5801";
                    assertNotEquals(firstMaster, secondMaster);
                    for (V1Pod worker : workers(first)) {
                        List<String> command = worker.getSpec().getContainers().get(0).getCommand();
                        assertTrue(command.contains(firstMaster));
                        assertFalse(command.contains(secondMaster));
                        assertEquals(
                                first.getApplicationId().getId(),
                                worker.getMetadata().getOwnerReferences().get(0).getName());
                    }
                    first.cancel();
                    assertEquals(ApplicationStatus.CANCELED, first.getStatus());
                    awaitAllResourcesRemoved(first);
                    assertEquals(ApplicationStatus.RUNNING, second.getStatus());
                    assertEquals(1, workers(second).size());
                } finally {
                    second.cancel();
                }
                awaitAllResourcesRemoved(second);
            } finally {
                first.cancel();
            }
            awaitAllResourcesRemoved(first);
        }
    }

    @Test
    void workerLossFailsApplicationAndCleansRemainingWorkers() throws Exception {
        try (ApplicationClient application =
                deployer.deploy(specification(job("STREAMING", "Console")))) {
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
            } finally {
                application.cancel();
            }
            awaitAllResourcesRemoved(application);
        }
    }

    @Test
    void durableCheckpointRestoresSourceProgressAfterWorkerFailure() throws Exception {
        String claim = "application-checkpoints";
        core.createNamespacedPersistentVolumeClaim(
                namespace,
                new V1PersistentVolumeClaim()
                        .metadata(new V1ObjectMeta().name(claim))
                        .spec(
                                new V1PersistentVolumeClaimSpec()
                                        .accessModes(Collections.singletonList("ReadWriteOnce"))
                                        .resources(
                                                new V1ResourceRequirements()
                                                        .requests(
                                                                Collections.singletonMap(
                                                                        "storage",
                                                                        Quantity.fromString(
                                                                                "1Gi"))))),
                null,
                null,
                null,
                null);
        Map<String, String> recovery = new HashMap<>(options);
        recovery.put(ApplicationOptions.WORKER_COUNT.key(), "1");
        recovery.put(KubernetesOptions.CHECKPOINT_PVC.key(), claim);
        String marker = "checkpoint-progress-marker";
        String config = readJobTemplate("checkpoint_recovery.conf", 1, marker);
        ApplicationSpecification firstSpecification =
                ApplicationSpecification.fromOptions(DeployType.KUBERNETES, config, recovery);
        try (ApplicationClient first = deployer.deploy(firstSpecification)) {
            try {
                awaitWorkersRunning(first, 1);
                String worker = workers(first).get(0).getMetadata().getName();
                Awaitility.await()
                        .atMost(180, TimeUnit.SECONDS)
                        .untilAsserted(() -> assertTrue(podLogs(first, worker).contains(marker)));
                // At most one checkpoint is pending; two further completions ensure that the
                // accepted checkpoint was triggered after the marker was observed.
                int completed =
                        completedCheckpoints(
                                podLogs(first, masterPod(first).getMetadata().getName()),
                                firstSpecification.getJobId());
                awaitDurableCheckpoint(first, firstSpecification.getJobId(), completed + 2);
                core.deleteNamespacedPod(
                        worker, namespace, null, null, 0, null, "Background", null);
                awaitStatus(first, ApplicationStatus.FAILED);
                awaitWorkersRemoved(first);
            } catch (Exception | AssertionError failure) {
                recordPodDiagnostics(first);
                throw failure;
            } finally {
                first.cancel();
            }
            awaitAllResourcesRemoved(first);
        }
        V1PersistentVolumeClaim retained =
                core.readNamespacedPersistentVolumeClaim(claim, namespace, null);
        assertEquals("Bound", retained.getStatus().getPhase());
        assertTrue(
                retained.getMetadata().getOwnerReferences() == null
                        || retained.getMetadata().getOwnerReferences().isEmpty());
        recovery.put(
                ApplicationOptions.RESTORE_JOB_ID.key(),
                Long.toString(firstSpecification.getJobId()));
        ApplicationSpecification restoredSpecification =
                ApplicationSpecification.fromOptions(DeployType.KUBERNETES, config, recovery);
        assertNotEquals(firstSpecification.getJobId(), restoredSpecification.getJobId());
        try (ApplicationClient restored = deployer.deploy(restoredSpecification)) {
            try {
                awaitWorkersRunning(restored, 1);
                awaitDurableCheckpoint(restored, restoredSpecification.getJobId(), 1);
                String restoredWorker = workers(restored).get(0).getMetadata().getName();
                Awaitility.await()
                        .atMost(30, TimeUnit.SECONDS)
                        .untilAsserted(
                                () -> {
                                    String logs = podLogs(restored, restoredWorker);
                                    assertFalse(
                                            logs.isEmpty(),
                                            "Restored worker logs must be readable");
                                    assertFalse(
                                            logs.contains(marker),
                                            "The restored source must not replay rows included in the durable checkpoint");
                                });
            } catch (Exception | AssertionError failure) {
                recordPodDiagnostics(restored);
                throw failure;
            } finally {
                restored.cancel();
            }
            awaitAllResourcesRemoved(restored);
        }
        assertEquals(
                "Bound",
                core.readNamespacedPersistentVolumeClaim(claim, namespace, null)
                        .getStatus()
                        .getPhase());
    }

    private static int completedCheckpoints(String logs, long jobId) {
        return logs.split(
                                Pattern.quote(
                                        "pending checkpoint notify finished, job id: "
                                                + jobId
                                                + ","),
                                -1)
                        .length
                - 1;
    }

    private void awaitDurableCheckpoint(
            ApplicationClient application, long jobId, int expectedCompletions) {
        try {
            Awaitility.await()
                    .atMost(180, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                V1Pod master = masterPod(application);
                                assertTrue(
                                        completedCheckpoints(
                                                        podLogs(
                                                                application,
                                                                master.getMetadata().getName()),
                                                        jobId)
                                                >= expectedCompletions);
                                assertTrue(
                                        checkpointFile(master, jobId)
                                                .contains(Long.toString(jobId)));
                            });
        } catch (RuntimeException failure) {
            recordPodDiagnostics(application);
            throw failure;
        }
    }

    private String checkpointFile(V1Pod master, long jobId) throws Exception {
        Process process =
                new Exec(apiClient)
                        .exec(
                                master,
                                new String[] {
                                    "find",
                                    "/opt/seatunnel/checkpoints",
                                    "-path",
                                    "*/" + jobId + "/*",
                                    "-type",
                                    "f",
                                    "!",
                                    "-name",
                                    "*tmp",
                                    "!",
                                    "-name",
                                    "*.crc",
                                    "-size",
                                    "+0c",
                                    "-print",
                                    "-quit"
                                },
                                "seatunnel",
                                false,
                                false);
        // Capture stdout before the SDK closes its WebSocket on process exit. Limit find to one
        // path so that waiting for exit cannot fill the SDK's stdout pipe and block completion.
        try (InputStream input = process.getInputStream();
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            assertTrue(
                    process.waitFor(15, TimeUnit.SECONDS),
                    "Checkpoint volume inspection timed out");
            assertEquals(0, process.exitValue());
            byte[] buffer = new byte[1024];
            int read;
            while ((read = input.read(buffer)) != -1) {
                output.write(buffer, 0, read);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        } finally {
            process.destroy();
        }
    }

    private String podLogs(ApplicationClient application, String name) throws Exception {
        ApplicationStatus status = application.getStatus();
        if (status.isTerminal() || status == ApplicationStatus.UNKNOWN) {
            throw new IllegalStateException(
                    "Application stopped while waiting for Pod " + name + " logs: " + status);
        }
        try {
            return core.readNamespacedPodLog(
                    name,
                    namespace,
                    "seatunnel",
                    false,
                    false,
                    null,
                    null,
                    false,
                    null,
                    null,
                    false);
        } catch (ApiException failure) {
            // Pod phase can become Running before kubelet makes its container log available.
            if (failure.getCode() == 400 || failure.getCode() == 404) {
                LOG.warn(
                        "Pod {} logs not yet available (HTTP {}): {}",
                        name,
                        failure.getCode(),
                        failure.getResponseBody());
                return "";
            }
            throw new ApiException(
                    "Cannot read logs for Pod "
                            + name
                            + " (HTTP "
                            + failure.getCode()
                            + "): "
                            + failure.getResponseBody(),
                    failure,
                    failure.getCode(),
                    failure.getResponseHeaders(),
                    failure.getResponseBody());
        }
    }

    @Test
    void quotaFailureDuringWorkerAllocationCleansPartialApplication() throws Exception {
        String quota = "partial-worker-allocation";
        core.createNamespacedResourceQuota(
                namespace,
                new V1ResourceQuota()
                        .metadata(new V1ObjectMeta().name(quota))
                        .spec(
                                new V1ResourceQuotaSpec()
                                        .hard(
                                                Collections.singletonMap(
                                                        "pods", Quantity.fromString("2")))),
                null,
                null,
                null,
                null);
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                V1ResourceQuota current =
                                        core.readNamespacedResourceQuota(quota, namespace, null);
                                return current.getStatus() != null
                                        && current.getStatus().getHard() != null;
                            });
            try (ApplicationClient application =
                    deployer.deploy(specification(job("STREAMING", "Console")))) {
                try {
                    awaitStatus(application, ApplicationStatus.FAILED);
                    awaitWorkersRemoved(application);
                } finally {
                    application.cancel();
                }
                awaitAllResourcesRemoved(application);
            }
        } finally {
            core.deleteNamespacedResourceQuota(
                    quota, namespace, null, null, 0, null, "Background", null);
        }
    }

    private void awaitStatus(ApplicationClient application, ApplicationStatus expected) {
        try {
            Awaitility.await()
                    .atMost(300, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(expected, application.getStatus()));
        } catch (RuntimeException e) {
            recordPodDiagnostics(application);
            throw e;
        }
    }

    private void recordPodDiagnostics(ApplicationClient application) {
        try {
            Path reports =
                    Paths.get(
                            PROJECT_ROOT_PATH,
                            "seatunnel-e2e",
                            "seatunnel-resource-managers-e2e",
                            "target",
                            "failsafe-reports");
            Files.createDirectories(reports);
            for (V1Pod pod : pods(application, null)) {
                String name = pod.getMetadata().getName();
                LOG.error("Application Pod {} status: {}", name, pod.getStatus());
                try {
                    String logs =
                            core.readNamespacedPodLog(
                                    name,
                                    namespace,
                                    "seatunnel",
                                    false,
                                    false,
                                    null,
                                    null,
                                    false,
                                    null,
                                    200,
                                    false);
                    Files.write(
                            reports.resolve(name + ".log"), logs.getBytes(StandardCharsets.UTF_8));
                    LOG.error("Application Pod {} logs: {}", name, logs);
                } catch (ApiException e) {
                    LOG.warn("Cannot read logs for Pod {}", name, e);
                }
            }
        } catch (Exception e) {
            LOG.warn("Cannot collect application diagnostics", e);
        }
    }

    private void awaitWorkersRunning(ApplicationClient application, int expected) {
        Awaitility.await()
                .atMost(240, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<V1Pod> workers = workers(application);
                            assertEquals(expected, workers.size());
                            assertTrue(
                                    workers.stream()
                                            .allMatch(
                                                    pod ->
                                                            pod.getStatus() != null
                                                                    && "Running"
                                                                            .equals(
                                                                                    pod.getStatus()
                                                                                            .getPhase())));
                        });
    }

    private void awaitJobRunning(ApplicationClient application) {
        try {
            Awaitility.await()
                    .atMost(180, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertTrue(
                                            podLogs(
                                                            application,
                                                            masterPod(application)
                                                                    .getMetadata()
                                                                    .getName())
                                                    .contains(
                                                            "turned from state SCHEDULED to RUNNING.")));
        } catch (RuntimeException failure) {
            recordPodDiagnostics(application);
            throw failure;
        }
    }

    private void awaitWorkersRemoved(ApplicationClient application) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> assertTrue(workers(application).isEmpty()));
    }

    private void awaitAllResourcesRemoved(ApplicationClient application) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertTrue(pods(application, null).isEmpty());
                            assertTrue(
                                    missing(
                                            () ->
                                                    batch.readNamespacedJob(
                                                            application.getApplicationId().getId(),
                                                            namespace,
                                                            null)));
                            assertTrue(
                                    missing(
                                            () ->
                                                    core.readNamespacedService(
                                                            application.getApplicationId().getId(),
                                                            namespace,
                                                            null)));
                            assertTrue(
                                    missing(
                                            () ->
                                                    core.readNamespacedConfigMap(
                                                            application.getApplicationId().getId(),
                                                            namespace,
                                                            null)));
                        });
    }

    private static boolean missing(ApiRead read) throws Exception {
        try {
            read.run();
            return false;
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                return true;
            }
            throw e;
        }
    }

    private interface ApiRead {
        void run() throws Exception;
    }

    private List<V1Pod> workers(ApplicationClient application) throws Exception {
        return pods(application, "worker");
    }

    private V1Pod masterPod(ApplicationClient application) throws Exception {
        return pods(application, "master").get(0);
    }

    private List<V1Pod> pods(ApplicationClient application, String role) throws Exception {
        String selector = APPLICATION_LABEL + "=" + application.getApplicationId().getId();
        if (role != null) {
            selector += "," + ROLE_LABEL + "=" + role;
        }
        return core.listNamespacedPod(
                        namespace, null, null, null, null, selector, null, null, null, null, null,
                        null)
                .getItems();
    }

    private ApplicationSpecification specification(String config) {
        return ApplicationSpecification.fromOptions(DeployType.KUBERNETES, config, options);
    }

    private ApplicationSpecification singleWorkerSpecification(String config) {
        Map<String, String> singleWorker = new HashMap<>(options);
        singleWorker.put(ApplicationOptions.WORKER_COUNT.key(), "1");
        return ApplicationSpecification.fromOptions(DeployType.KUBERNETES, config, singleWorker);
    }

    private void importImage(String image) throws Exception {
        LOG.info("Importing application image {} into disposable K3s", image);
        Path archive = Files.createTempFile("seatunnel-k3s-image-", ".tar");
        try {
            try (InputStream input = dockerClient.saveImageCmd(image).exec()) {
                Files.copy(input, archive, StandardCopyOption.REPLACE_EXISTING);
            }
            k3s.copyFileToContainer(
                    MountableFile.forHostPath(archive), "/tmp/seatunnel-application-image.tar");
            Container.ExecResult imported =
                    k3s.execInContainer(
                            "ctr",
                            "--address",
                            "/run/k3s/containerd/containerd.sock",
                            "--namespace",
                            "k8s.io",
                            "images",
                            "import",
                            "/tmp/seatunnel-application-image.tar");
            assertEquals(0, imported.getExitCode(), imported.getStderr());
            k3s.execInContainer("rm", "/tmp/seatunnel-application-image.tar");
        } finally {
            Files.deleteIfExists(archive);
        }
    }

    private static String job(String mode, String sink) throws IOException {
        return job(mode, sink, 2);
    }

    private static String job(String mode, String sink, int parallelism) throws IOException {
        String template =
                "ConnectorThatDoesNotExist".equals(sink)
                        ? "invalid_sink.conf"
                        : "STREAMING".equals(mode) ? "fake_streaming.conf" : "fake_batch.conf";
        return readJobTemplate(template, parallelism, "APPLICATION_E2E_DATA");
    }

    private static String assertSubmissionJob() throws IOException {
        return readJobTemplate("fake_batch.conf", 2, "APPLICATION_E2E_DATA");
    }

    private static String readJobTemplate(String name, int parallelism, String marker)
            throws IOException {
        String configuration =
                new String(
                                Files.readAllBytes(getResourcesFile("/common/" + name).toPath()),
                                StandardCharsets.UTF_8)
                        .replace("{{parallelism}}", Integer.toString(parallelism))
                        .replace("{{row_count}}", Integer.toString(parallelism))
                        .replace("{{split_count}}", Integer.toString(parallelism))
                        .replace("{{marker}}", marker);
        assertFalse(configuration.contains("{{"), "Unresolved job configuration variable");
        return configuration;
    }

    private String buildApplicationImage() throws Exception {
        LOG.info("Building application image from staged Maven test dependencies");
        Path context = Files.createTempDirectory("seatunnel-kubernetes-image-");
        try {
            Path home = context.resolve("seatunnel");
            Path starter = Files.createDirectories(home.resolve("starter"));
            Path lib = Files.createDirectories(home.resolve("lib"));
            Path connectors = Files.createDirectories(home.resolve("connectors"));
            Path platform = Files.createDirectories(home.resolve("resource-managers/kubernetes"));
            Files.copy(
                    DependencyJar.staged("seatunnel-starter.jar").path(),
                    starter.resolve("seatunnel-starter.jar"));
            for (String name : Arrays.asList("seatunnel-shade-hadoop3-uber.jar")) {
                Files.copy(DependencyJar.staged(name).path(), lib.resolve(name));
            }
            for (String connector : Arrays.asList("fake", "console", "assert")) {
                String name = "connector-" + connector + ".jar";
                Files.copy(DependencyJar.staged(name).path(), connectors.resolve(name));
            }
            Files.copy(
                    DependencyJar.staged("seatunnel-resource-manager-kubernetes.jar").path(),
                    platform.resolve("seatunnel-resource-manager-kubernetes.jar"));
            FileUtils.copyDirectory(
                    Paths.get(PROJECT_ROOT_PATH, "config").toFile(),
                    home.resolve("config").toFile());
            FileUtils.copyDirectory(
                    Paths.get(PROJECT_ROOT_PATH, "seatunnel-core/seatunnel-starter/src/main/bin")
                            .toFile(),
                    home.resolve("bin").toFile());
            Files.copy(
                    Paths.get(PROJECT_ROOT_PATH, "plugin-mapping.properties"),
                    connectors.resolve("plugin-mapping.properties"));
            Files.copy(
                    getResourcesFile("/kubernetes/seatunnel.yaml").toPath(),
                    home.resolve("config/seatunnel.yaml"),
                    StandardCopyOption.REPLACE_EXISTING);
            Files.write(
                    context.resolve("Dockerfile"),
                    Arrays.asList(
                            "FROM eclipse-temurin:8-jdk",
                            "COPY seatunnel/ /opt/seatunnel/",
                            "ENV SEATUNNEL_HOME=/opt/seatunnel",
                            "WORKDIR /opt/seatunnel"),
                    StandardCharsets.UTF_8);
            String image =
                    "seatunnel-application-it:" + UUID.randomUUID().toString().substring(0, 8);
            dockerClient
                    .buildImageCmd(context.toFile())
                    .withTags(Collections.singleton(image))
                    .start()
                    .awaitImageId();
            return image;
        } finally {
            FileUtils.deleteDirectory(context.toFile());
        }
    }
}
