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
import org.apache.seatunnel.engine.client.deployment.ApplicationClusterDeployer;
import org.apache.seatunnel.engine.client.deployment.ClusterDescriptor;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.kubernetes.KubernetesApplicationClusterDescriptorFactory;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationClient;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClientFactory;
import org.apache.seatunnel.resource.kubernetes.worker.SeatunnelKubernetesApplicationWorker;

import org.codehaus.plexus.util.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import io.kubernetes.client.Exec;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1ResourceQuota;
import io.kubernetes.client.openapi.models.V1Role;
import io.kubernetes.client.openapi.models.V1RoleBinding;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1ServiceAccount;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Yaml;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.StringReader;
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
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.getResourcesFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs real application processes in a disposable K3s cluster using Maven test dependencies. */
@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK, EngineType.SPARK, EngineType.SEATUNNEL})
public class KubernetesApplicationIT extends TestSuiteBase {
    @TempDir static Path temporary;
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesApplicationIT.class);
    private static final String APPLICATION_LABEL = "seatunnel.apache.org/application-id";
    private static final String ROLE_LABEL = "seatunnel.apache.org/role";
    private static final String APPLICATION_SPECIFICATION_FILE = "application.properties";
    private static final String RUNTIME_CONFIG_MAP = "seatunnel-runtime-configuration";
    private static final String K3S_IMAGE = "rancher/k3s:v1.31.6-k3s1";
    private String namespace;
    private CoreV1Api core;
    private BatchV1Api batch;
    private ClusterDescriptor<String> deployer;
    private boolean namespaceCreated;
    private ApiClient apiClient;
    private KubernetesClient platformMonitor;
    private K3sContainer k3s;
    private Path kubeconfig;
    private Path applicationConfig;

    @BeforeAll
    void prepareApplicationEnvironment() throws Exception {
        applicationConfig = getResourcesFile("/kubernetes/batch/application.config").toPath();
        Map<String, String> options =
                SeatunnelApplicationConfig.load(applicationConfig, Collections.emptyMap());
        namespace = options.get(KubernetesOptions.NAMESPACE.key());
        kubeconfig = Paths.get(options.get(KubernetesOptions.KUBE_CONFIG.key()));
        String image = options.get(KubernetesOptions.IMAGE.key());
        buildApplicationImage(image);
        k3s =
                new K3sContainer(DockerImageName.parse(K3S_IMAGE))
                        .withStartupTimeout(Duration.ofMinutes(3))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(K3S_IMAGE)));
        k3s.start();
        Files.createDirectories(kubeconfig.getParent());
        Files.write(kubeconfig, k3s.getKubeConfigYaml().getBytes(StandardCharsets.UTF_8));
        importImage(image);
        // The isolation case runs two masters and two workers: four CPUs and 3 GiB in total.
        // Keep this within the standard public GitHub Actions runner's four CPUs. Remove
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
                Yaml.loadAs(
                        getResourcesFile("/kubernetes/service-account.yaml"),
                        V1ServiceAccount.class),
                null,
                null,
                null,
                null);
        Map<String, String> runtimeConfiguration = new HashMap<>();
        runtimeConfiguration.put(
                "seatunnel.yaml",
                new String(
                        Files.readAllBytes(getResourcesFile("/kubernetes/seatunnel.yaml").toPath()),
                        StandardCharsets.UTF_8));
        runtimeConfiguration.put(
                "log4j2_client.properties",
                new String(
                        Files.readAllBytes(
                                Paths.get(PROJECT_ROOT_PATH, "config", "log4j2_client.properties")),
                        StandardCharsets.UTF_8));
        core.createNamespacedConfigMap(
                namespace,
                new V1ConfigMap()
                        .metadata(new V1ObjectMeta().name(RUNTIME_CONFIG_MAP))
                        .data(runtimeConfiguration),
                null,
                null,
                null,
                null);
        RbacAuthorizationV1Api rbac = new RbacAuthorizationV1Api(apiClient);
        rbac.createNamespacedRole(
                namespace,
                Yaml.loadAs(getResourcesFile("/kubernetes/role.yaml"), V1Role.class),
                null,
                null,
                null,
                null);
        V1RoleBinding roleBinding =
                Yaml.loadAs(getResourcesFile("/kubernetes/role-binding.yaml"), V1RoleBinding.class);
        rbac.createNamespacedRoleBinding(namespace, roleBinding, null, null, null, null);
        deployer = new KubernetesApplicationClusterDescriptorFactory().create(options);
        platformMonitor = KubernetesClientFactory.create(options, false);
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
                if (platformMonitor != null) {
                    platformMonitor.close();
                }
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
        ApplicationSpecification specification = specification("batch");
        try (KubernetesApplicationClient application = deployApplication(specification, "batch")) {
            try {
                awaitStatus(application, ApplicationStatus.SUCCEEDED);
                awaitWorkersRemoved(application);
                assertEquals(
                        ApplicationStatus.SUCCEEDED,
                        application.getStatus(),
                        "Runner cleanup must preserve the successful application result");
                assertEquals(
                        1,
                        batch.readNamespacedJob(application.getClusterId(), namespace, null)
                                .getStatus()
                                .getSucceeded());
                V1Secret applicationSecret =
                        core.readNamespacedSecret(application.getClusterId(), namespace, null);
                assertEquals(1, applicationSecret.getMetadata().getOwnerReferences().size());
                assertEquals("Opaque", applicationSecret.getType());
                assertTrue(applicationSecret.getData().containsKey(APPLICATION_SPECIFICATION_FILE));
                Properties runtime = new Properties();
                runtime.load(
                        new StringReader(
                                new String(
                                        applicationSecret
                                                .getData()
                                                .get(APPLICATION_SPECIFICATION_FILE),
                                        StandardCharsets.UTF_8)));
                assertEquals("V1", runtime.getProperty("format.version"));
                assertEquals(
                        Long.toString(specification.getJobId()),
                        runtime.getProperty(ApplicationOptions.JOB_ID.key()));
                assertEquals(namespace, runtime.getProperty(KubernetesOptions.NAMESPACE.key()));
                assertEquals(
                        "Never", runtime.getProperty(KubernetesOptions.IMAGE_PULL_POLICY.key()));
                assertFalse(runtime.containsKey(KubernetesOptions.KUBE_CONFIG.key()));
                assertStatusFromApplicationCli(application.getClusterId());
                assertTrue(
                        missing(
                                () ->
                                        core.readNamespacedConfigMap(
                                                application.getClusterId(), namespace, null)));
            } finally {
                deployer.cancelApplication(application.getClusterId());
            }
            awaitAllResourcesRemoved(application);
        }
    }

    @Test
    void invalidJobFailsAndReleasesWorkers() throws Exception {
        try (KubernetesApplicationClient application = deployApplication("invalid-job")) {
            try {
                awaitStatus(application, ApplicationStatus.FAILED);
                awaitWorkersRemoved(application);
                String masterLogs = podLogs(masterPod(application).getMetadata().getName());
                assertTrue(
                        masterLogs.contains("NonexistentSink"),
                        () ->
                                "The application must reach job parsing, not fail during runtime startup. Master logs:\n"
                                        + masterLogs);
            } finally {
                deployer.cancelApplication(application.getClusterId());
            }
            awaitAllResourcesRemoved(application);
        }
    }

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
                    deployer.cancelApplication(first.getClusterId());
                    awaitAllResourcesRemoved(first);
                    assertEquals(
                            ApplicationStatus.UNKNOWN,
                            deployer.getApplicationStatus(first.getClusterId()));
                    awaitAllResourcesRemoved(first);
                    assertEquals(
                            ApplicationStatus.RUNNING,
                            deployer.getApplicationStatus(second.getClusterId()));
                    assertEquals(1, workers(second).size());
                } finally {
                    deployer.cancelApplication(second.getClusterId());
                }
                awaitAllResourcesRemoved(second);
            } finally {
                deployer.cancelApplication(first.getClusterId());
            }
            awaitAllResourcesRemoved(first);
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
                deployer.cancelApplication(application.getClusterId());
            }
            awaitAllResourcesRemoved(application);
        }
    }

    @Test
    void masterExitTerminatesWorkersWithoutMasterCleanup() throws Exception {
        try (KubernetesApplicationClient application = deployApplication("isolation")) {
            try {
                awaitWorkersRunning(application, 1);
                awaitJobRunning(application);
                V1Pod master = masterPod(application);
                killMasterContainer(master);
                awaitStatus(application, ApplicationStatus.FAILED);
                awaitWorkersExited(application);
            } finally {
                deployer.cancelApplication(application.getClusterId());
            }
            awaitAllResourcesRemoved(application);
        }
    }

    @Test
    void durableCheckpointRestoresSourceProgressAfterWorkerFailure() throws Exception {
        V1PersistentVolumeClaim checkpointVolume =
                Yaml.loadAs(
                        getResourcesFile("/kubernetes/checkpoints-pvc.yaml"),
                        V1PersistentVolumeClaim.class);
        String claim = checkpointVolume.getMetadata().getName();
        core.createNamespacedPersistentVolumeClaim(
                namespace, checkpointVolume, null, null, null, null);
        String marker = "APPLICATION_E2E_DATA";
        ApplicationSpecification firstSpecification = specification("checkpoint");
        try (KubernetesApplicationClient first =
                deployApplication(firstSpecification, "checkpoint")) {
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
                deployer.cancelApplication(first.getClusterId());
            }
            awaitAllResourcesRemoved(first);
        }
        V1PersistentVolumeClaim retained =
                core.readNamespacedPersistentVolumeClaim(claim, namespace, null);
        assertEquals("Bound", retained.getStatus().getPhase());
        assertTrue(
                retained.getMetadata().getOwnerReferences() == null
                        || retained.getMetadata().getOwnerReferences().isEmpty());
        ApplicationSpecification restoredSpecification = specification("checkpoint-restore");
        assertEquals(firstSpecification.getJobId(), restoredSpecification.getRestoreJobId());
        assertNotEquals(firstSpecification.getJobId(), restoredSpecification.getJobId());
        try (KubernetesApplicationClient restored =
                deployApplication(restoredSpecification, "checkpoint-restore")) {
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
                deployer.cancelApplication(restored.getClusterId());
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
            KubernetesApplicationClient application, long jobId, int expectedCompletions) {
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

    private String podLogs(KubernetesApplicationClient application, String name) throws Exception {
        ApplicationStatus status = application.getStatus();
        if (status.isTerminal() || status == ApplicationStatus.UNKNOWN) {
            throw new IllegalStateException(
                    "Application stopped while waiting for Pod " + name + " logs: " + status);
        }
        return podLogs(name);
    }

    /** Reads retained Pod logs even after the application has finished, for failure diagnostics. */
    private String podLogs(String name) throws Exception {
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
        V1ResourceQuota workerQuota =
                Yaml.loadAs(
                        getResourcesFile("/kubernetes/worker-quota.yaml"), V1ResourceQuota.class);
        String quota = workerQuota.getMetadata().getName();
        core.createNamespacedResourceQuota(namespace, workerQuota, null, null, null, null);
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
            try (KubernetesApplicationClient application =
                    deployApplicationWithoutStartupWait(specification("quota"), "quota")) {
                try {
                    awaitStatus(application, ApplicationStatus.FAILED);
                    awaitWorkersRemoved(application);
                } finally {
                    deployer.cancelApplication(application.getClusterId());
                }
                awaitAllResourcesRemoved(application);
            }
        } finally {
            core.deleteNamespacedResourceQuota(
                    quota, namespace, null, null, 0, null, "Background", null);
        }
    }

    private KubernetesApplicationClient deployApplication(String scenario) throws Exception {
        return deployApplication(specification(scenario), scenario);
    }

    private KubernetesApplicationClient deployApplication(
            ApplicationSpecification specification, String scenario) throws Exception {

        Map<String, String> options =
                SeatunnelApplicationConfig.load(
                        getResourcesFile("/kubernetes/" + scenario + "/application.config")
                                .toPath(),
                        Collections.emptyMap());

        ApplicationClusterDeployer deployer =
                new ApplicationClusterDeployer(DeployType.KUBERNETES, specification, options);

        return new KubernetesApplicationClient(platformMonitor, deployer.run());
    }

    private KubernetesApplicationClient deployApplicationWithoutStartupWait(
            ApplicationSpecification specification, String scenario) throws Exception {
        Map<String, String> options =
                SeatunnelApplicationConfig.load(
                        getResourcesFile("/kubernetes/" + scenario + "/application.config")
                                .toPath(),
                        Collections.emptyMap());
        String id =
                new KubernetesApplicationClusterDescriptorFactory()
                        .deployApplicationWithoutWaitingForStartup(specification, options);
        return new KubernetesApplicationClient(platformMonitor, id);
    }

    private void awaitStatus(KubernetesApplicationClient application, ApplicationStatus expected) {
        try {
            Awaitility.await()
                    .atMost(300, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertEquals(
                                            expected,
                                            deployer.getApplicationStatus(
                                                    application.getClusterId())));
        } catch (RuntimeException e) {
            recordPodDiagnostics(application);
            throw e;
        }
    }

    private void recordPodDiagnostics(KubernetesApplicationClient application) {
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

    private void awaitWorkersRunning(KubernetesApplicationClient application, int expected) {
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

    private void awaitJobRunning(KubernetesApplicationClient application) {
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

    private void awaitWorkersRemoved(KubernetesApplicationClient application) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> assertTrue(workers(application).isEmpty()));
    }

    private void awaitWorkersExited(KubernetesApplicationClient application) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
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

    private void killMasterContainer(V1Pod master) throws Exception {
        String containerId = master.getStatus().getContainerStatuses().get(0).getContainerID();
        String runtimeId = containerId.substring("containerd://".length());
        Container.ExecResult result =
                k3s.execInContainer(
                        "ctr",
                        "--address",
                        "/run/k3s/containerd/containerd.sock",
                        "--namespace",
                        "k8s.io",
                        "tasks",
                        "kill",
                        "--signal",
                        "SIGKILL",
                        runtimeId);
        assertEquals(0, result.getExitCode(), result.getStderr());
    }

    private static void assertRuntimeConfigMapMounted(V1Pod pod) {
        assertTrue(
                pod.getSpec().getVolumes().stream()
                        .anyMatch(
                                volume ->
                                        volume.getConfigMap() != null
                                                && RUNTIME_CONFIG_MAP.equals(
                                                        volume.getConfigMap().getName())));
        assertTrue(
                pod.getSpec().getContainers().get(0).getVolumeMounts().stream()
                        .anyMatch(
                                mount ->
                                        "/opt/seatunnel/config".equals(mount.getMountPath())
                                                && Boolean.TRUE.equals(mount.getReadOnly())));
    }

    private void awaitAllResourcesRemoved(KubernetesApplicationClient application) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertTrue(pods(application, null).isEmpty());
                            assertTrue(
                                    missing(
                                            () ->
                                                    batch.readNamespacedJob(
                                                            application.getClusterId(),
                                                            namespace,
                                                            null)));
                            assertTrue(
                                    missing(
                                            () ->
                                                    core.readNamespacedService(
                                                            application.getClusterId(),
                                                            namespace,
                                                            null)));
                            assertTrue(
                                    missing(
                                            () ->
                                                    core.readNamespacedSecret(
                                                            application.getClusterId(),
                                                            namespace,
                                                            null)));
                            assertTrue(
                                    missing(
                                            () ->
                                                    core.readNamespacedConfigMap(
                                                            application.getClusterId(),
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

    private List<V1Pod> workers(KubernetesApplicationClient application) throws Exception {
        return pods(application, "worker");
    }

    private V1Pod masterPod(KubernetesApplicationClient application) throws Exception {
        return pods(application, "master").get(0);
    }

    private List<V1Pod> pods(KubernetesApplicationClient application, String role)
            throws Exception {
        String selector = APPLICATION_LABEL + "=" + application.getClusterId();
        if (role != null) {
            selector += "," + ROLE_LABEL + "=" + role;
        }
        return core.listNamespacedPod(
                        namespace, null, null, null, null, selector, null, null, null, null, null,
                        null)
                .getItems();
    }

    /** Queries a finished application through the packaged CLI without connecting to its master. */
    private void assertStatusFromApplicationCli(String applicationId) throws Exception {
        String classpath =
                DependencyJar.staged("seatunnel-starter.jar").path()
                        + File.pathSeparator
                        + DependencyJar.staged("seatunnel-resource-manager-kubernetes.jar").path();
        Path output = temporary.resolve("application-status.log");
        Process process =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "java")
                                        .toString(),
                                "-cp",
                                classpath,
                                "org.apache.seatunnel.core.starter.seatunnel.SeaTunnelApplication",
                                "status",
                                "-t",
                                "kubernetes",
                                "--id",
                                applicationId,
                                "-a",
                                applicationConfig.toString())
                        .redirectErrorStream(true)
                        .redirectOutput(output.toFile())
                        .start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Application CLI did not finish");
            String text = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
            assertEquals(0, process.exitValue(), text);
            assertTrue(text.contains("Status: SUCCEEDED"), text);
        } finally {
            process.destroyForcibly();
        }
    }

    /** Reads the two complete scenario files directly, without option overrides or templates. */
    private ApplicationSpecification specification(String scenario) {
        return SeatunnelApplicationConfig.parse(
                getResourcesFile("/kubernetes/" + scenario + "/job.conf").toPath(),
                SeatunnelApplicationConfig.load(
                        getResourcesFile("/kubernetes/" + scenario + "/application.config")
                                .toPath(),
                        Collections.emptyMap()));
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

    private void buildApplicationImage(String image) throws Exception {
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
            Files.copy(
                    DependencyJar.staged("seatunnel-shade-hadoop3-uber.jar").path(),
                    lib.resolve("seatunnel-shade-hadoop3-uber.jar"));
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
                    getResourcesFile("/kubernetes/Dockerfile").toPath(),
                    context.resolve("Dockerfile"));
            dockerClient
                    .buildImageCmd(context.toFile())
                    .withTags(Collections.singleton(image))
                    .start()
                    .awaitImageId();
        } finally {
            FileUtils.deleteDirectory(context.toFile());
        }
    }
}
