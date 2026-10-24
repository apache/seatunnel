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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.junit.TimingExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.engine.client.deployment.ApplicationClusterDeployer;
import org.apache.seatunnel.engine.client.deployment.ClusterDescriptor;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.kubernetes.KubernetesApplicationClusterDescriptorFactory;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationClient;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClientFactory;

import org.codehaus.plexus.util.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.github.dockerjava.api.DockerClient;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.CoreV1Event;
import io.kubernetes.client.openapi.models.CoreV1EventList;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobCondition;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Role;
import io.kubernetes.client.openapi.models.V1RoleBinding;
import io.kubernetes.client.openapi.models.V1ServiceAccount;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Yaml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.getResourcesFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Disposable K3s cluster, shared fixtures and test-facing helpers for the application-mode
 * integration scenarios.
 *
 * <p>{@link #startUp()} builds the application image and starts the cluster on first use; the
 * cluster is shared by every scenario class in the JVM because that setup costs minutes, and the
 * shutdown hook removes it after the suite. Scenario classes extend this base, override {@link
 * #startScenarioFixtures()} when they need class-scoped fixtures, and only contain scenario tests;
 * the helpers here cover the polling, inspection and assertion work they share.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TimingExtension.class)
public abstract class AbstractKubernetesApplicationIT implements TestResource {
    private static final String K3S_IMAGE = "rancher/k3s:v1.31.6-k3s1";
    private static final String CONTAINER_NAME = "seatunnel";
    protected static final String CONTAINERD_SOCKET = "/run/k3s/containerd/containerd.sock";
    protected static final long STATUS_TIMEOUT_SECONDS = 300;
    private static final long WORKERS_TIMEOUT_SECONDS = 240;
    protected static final long CLEANUP_TIMEOUT_SECONDS = 60;
    protected static final String APPLICATION_LABEL = "seatunnel.apache.org/application-id";
    protected static final String ROLE_LABEL = "seatunnel.apache.org/role";
    protected static final String RUNTIME_CONFIG_MAP = "seatunnel-runtime-configuration";
    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractKubernetesApplicationIT.class);

    private static boolean started;
    protected static String namespace;
    protected static CoreV1Api core;
    protected static BatchV1Api batch;
    protected static ApiClient apiClient;
    protected static ClusterDescriptor<String> deployer;
    private static K3sContainer k3s;
    private static Path kubeconfig;
    private static Network clusterNetwork;
    private static final String CGROUP_PREFIX = "seatunnel-e2e-" + java.util.UUID.randomUUID();
    private static final Map<String, GenericContainer<?>> computeNodes = new HashMap<>();
    private static final java.util.Set<String> unavailableNodes = new java.util.HashSet<>();
    protected static final boolean MULTI_NODE =
            Boolean.getBoolean("seatunnel.kubernetes.e2e.multinode");
    private static GenericContainer<?> storage;
    private static String storageEndpoint;
    private static final String STORAGE_IMAGE =
            "ghcr.io/teableio/minio@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e";
    protected static KubernetesClient platformMonitor;
    private static boolean namespaceCreated;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        startCluster();
        startScenarioFixtures();
    }

    /**
     * Releases the class-scoped fixtures only. The disposable cluster is JVM-scoped: it is removed
     * by the shutdown hook registered in {@link #startCluster()} because the scenario classes that
     * run later in the same JVM still need it.
     */
    @AfterAll
    @Override
    public void tearDown() throws Exception {
        stopScenarioFixtures();
    }

    /**
     * Cancels the application and waits until every resource it owned is gone. Cancellation is safe
     * to call after a failed test, so scenarios use this in {@code finally} blocks.
     */
    protected static void cancelAndAwait(KubernetesApplicationClient application) throws Exception {
        try {
            deployer.cancelApplication(application.getClusterId());
            awaitAllResourcesRemoved(application);
        } catch (Exception | Error failure) {
            recordApplicationDiagnostics(application);
            throw failure;
        }
    }

    /**
     * Scenario classes override to create the class-scoped fixtures their tests need. The shared
     * cluster is already available when this hook runs; the base environment stays generic and the
     * cluster itself is removed by its JVM shutdown hook after the suite.
     */
    protected void startScenarioFixtures() throws Exception {}

    /** Scenario classes override to release what {@link #startScenarioFixtures()} created. */
    protected void stopScenarioFixtures() throws Exception {}

    /** Returns one of the suite-level Kubernetes resources of {@code test/resources/kubernetes}. */
    protected static File suiteFile(String name) {
        return getResourcesFile("/kubernetes/" + name);
    }

    /** Returns one of the files a scenario keeps under {@code test/resources/kubernetes/<name>}. */
    protected static File scenarioFile(String scenario, String name) {
        return getResourcesFile("/kubernetes/" + scenario + "/" + name);
    }

    /** Runs one command inside the disposable cluster for scenario-level fault injection. */
    protected static Container.ExecResult execInCluster(String... command) throws Exception {
        return k3s.execInContainer(command);
    }

    protected static KubernetesApplicationClient deployApplication(String scenario)
            throws Exception {
        return deployApplication(specification(scenario), scenario);
    }

    protected static KubernetesApplicationClient deployApplication(
            ApplicationSpecification specification, String scenario) throws Exception {
        Map<String, String> options =
                SeatunnelApplicationConfig.load(
                        scenarioFile(scenario, "application.config").toPath(),
                        Collections.emptyMap());
        return deployApplication(specification, options);
    }

    protected static KubernetesApplicationClient deployApplication(
            ApplicationSpecification specification, Map<String, String> options) throws Exception {
        ApplicationClusterDeployer applicationDeployer =
                new ApplicationClusterDeployer(DeployType.KUBERNETES, specification, options);
        return new KubernetesApplicationClient(platformMonitor, applicationDeployer.run());
    }

    protected static void awaitStatus(
            KubernetesApplicationClient application, ApplicationStatus expected) {
        withPodDiagnostics(
                application,
                () ->
                        Awaitility.await()
                                .atMost(STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                                .pollInterval(2, TimeUnit.SECONDS)
                                .untilAsserted(
                                        () ->
                                                assertEquals(
                                                        expected,
                                                        deployer.getApplicationStatus(
                                                                application.getClusterId()))));
    }

    /** Runs a polling assertion and captures the application Pods when it fails. */
    protected static void withPodDiagnostics(
            KubernetesApplicationClient application, Runnable assertion) {
        try {
            assertion.run();
        } catch (RuntimeException | Error failure) {
            recordApplicationDiagnostics(application);
            throw failure;
        }
    }

    /**
     * Captures everything needed to explain a failure: the application status, the Job status and
     * conditions, the Job events and the retained Pod logs. The Pods may already be gone when a
     * startup failure is detected, so the Job state and events are the durable evidence.
     */
    protected static void recordApplicationDiagnostics(KubernetesApplicationClient application) {
        recordApplicationDiagnostics(application, 200);
    }

    protected static void recordApplicationDiagnostics(
            KubernetesApplicationClient application, Integer tailLines) {
        String applicationId = application.getClusterId();
        try {
            LOG.error("Application {} status: {}", applicationId, application.getStatus());
        } catch (Exception failure) {
            LOG.warn("Cannot read the status of application {}", applicationId, failure);
        }
        recordJobDiagnostics(applicationId);
        try {
            Path reports = diagnosticsDirectory();
            Files.createDirectories(reports);
            for (V1Pod pod : pods(application, null)) {
                String name = pod.getMetadata().getName();
                LOG.error("Application Pod {} status: {}", name, pod.getStatus());
                try {
                    String logs =
                            core.readNamespacedPodLog(
                                    name,
                                    namespace,
                                    CONTAINER_NAME,
                                    false,
                                    false,
                                    null,
                                    null,
                                    false,
                                    null,
                                    tailLines,
                                    false);
                    Files.write(
                            reports.resolve(name + ".log"),
                            (logs == null ? "" : logs).getBytes(StandardCharsets.UTF_8));
                    LOG.error("Application Pod {} logs: {}", name, logs);
                } catch (ApiException e) {
                    LOG.warn("Cannot read logs for Pod {}", name, e);
                }
                if (pod.getStatus() != null
                        && pod.getStatus().getContainerStatuses() != null
                        && pod.getStatus().getContainerStatuses().stream()
                                .anyMatch(
                                        status ->
                                                status.getRestartCount() != null
                                                        && status.getRestartCount() > 0)) {
                    try {
                        String previous =
                                core.readNamespacedPodLog(
                                        name,
                                        namespace,
                                        CONTAINER_NAME,
                                        false,
                                        false,
                                        null,
                                        null,
                                        true,
                                        null,
                                        tailLines,
                                        false);
                        Files.writeString(
                                reports.resolve(name + "-previous.log"),
                                previous == null ? "" : previous);
                    } catch (ApiException unavailable) {
                        LOG.warn(
                                "Cannot read previous container logs for Pod {}",
                                name,
                                unavailable);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Cannot collect application diagnostics", e);
        }
    }

    private static void recordJobDiagnostics(String applicationId) {
        try {
            V1Job job = batch.readNamespacedJob(applicationId, namespace, null);
            LOG.error("Application Job {} status: {}", applicationId, job.getStatus());
            if (job.getStatus() != null && job.getStatus().getConditions() != null) {
                for (V1JobCondition condition : job.getStatus().getConditions()) {
                    LOG.error(
                            "Application Job {} condition: type={}, status={}, reason={}, message={}",
                            applicationId,
                            condition.getType(),
                            condition.getStatus(),
                            condition.getReason(),
                            condition.getMessage());
                }
            }
        } catch (Exception failure) {
            LOG.warn("Cannot read the Job of application {}", applicationId, failure);
        }
        try {
            CoreV1EventList events =
                    core.listNamespacedEvent(
                            namespace,
                            null,
                            null,
                            null,
                            "involvedObject.name=" + applicationId,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null);
            for (CoreV1Event event : events.getItems()) {
                LOG.error(
                        "Application event: type={}, reason={}, count={}, message={}",
                        event.getType(),
                        event.getReason(),
                        event.getCount(),
                        event.getMessage());
            }
        } catch (Exception failure) {
            LOG.warn("Cannot read the events of application {}", applicationId, failure);
        }
    }

    protected static void awaitWorkersRunning(
            KubernetesApplicationClient application, int expected) {
        withPodDiagnostics(
                application,
                () ->
                        Awaitility.await()
                                .atMost(WORKERS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                                .pollInterval(2, TimeUnit.SECONDS)
                                .untilAsserted(
                                        () -> {
                                            ApplicationStatus status = application.getStatus();
                                            assertFalse(
                                                    status.isTerminal(),
                                                    () ->
                                                            "Application is "
                                                                    + status
                                                                    + " while waiting for "
                                                                    + expected
                                                                    + " workers");
                                            assertEquals(
                                                    1,
                                                    pods(application, "master").size(),
                                                    "The master Pod must stay alive while its workers start");
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
                                        }));
    }

    protected static void awaitWorkersRemoved(KubernetesApplicationClient application) {
        withPodDiagnostics(
                application,
                () ->
                        Awaitility.await()
                                .atMost(CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                                .untilAsserted(() -> assertTrue(workers(application).isEmpty())));
    }

    /** Checks that {@code path} is mounted into the Pod's container as a read-only file. */
    protected static boolean mountedReadOnly(V1Pod pod, String path) {
        return pod.getSpec().getContainers().get(0).getVolumeMounts().stream()
                .anyMatch(
                        mount ->
                                path.equals(mount.getMountPath())
                                        && Boolean.TRUE.equals(mount.getReadOnly()));
    }

    /** Failure diagnostics land next to the Maven integration-test reports. */
    private static Path diagnosticsDirectory() {
        String baseDir =
                System.getProperty(
                        "basedir",
                        PROJECT_ROOT_PATH + "/seatunnel-e2e/seatunnel-resource-managers-e2e");
        return Paths.get(baseDir, "target", "failsafe-reports");
    }

    protected static void awaitAllResourcesRemoved(KubernetesApplicationClient application) {
        awaitAllResourcesRemoved(application, CLEANUP_TIMEOUT_SECONDS);
    }

    protected static void awaitAllResourcesRemoved(
            KubernetesApplicationClient application, long timeoutSeconds) {
        Awaitility.await()
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(() -> assertAllResourcesRemoved(application));
    }

    protected static void assertAllResourcesRemoved(KubernetesApplicationClient application)
            throws Exception {
        assertTrue(pods(application, null).isEmpty());
        assertTrue(
                missing(
                        () ->
                                batch.readNamespacedJob(
                                        application.getClusterId(), namespace, null)));
        assertTrue(
                missing(
                        () ->
                                core.readNamespacedService(
                                        application.getClusterId(), namespace, null)));
        assertTrue(
                missing(
                        () ->
                                core.readNamespacedSecret(
                                        application.getClusterId(), namespace, null)));
        assertTrue(
                missing(
                        () ->
                                core.readNamespacedConfigMap(
                                        application.getClusterId(), namespace, null)));
    }

    protected static boolean missing(ApiRead read) throws Exception {
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

    protected interface ApiRead {
        void run() throws Exception;
    }

    protected static List<V1Pod> workers(KubernetesApplicationClient application) throws Exception {
        return pods(application, "worker");
    }

    protected static V1Pod masterPod(KubernetesApplicationClient application) throws Exception {
        return pods(application, "master").get(0);
    }

    private static List<V1Pod> pods(KubernetesApplicationClient application, String role)
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

    /** Reads the two complete scenario files directly, without option overrides or templates. */
    protected static ApplicationSpecification specification(String scenario) {
        return SeatunnelApplicationConfig.parse(
                scenarioFile(scenario, "job.conf").toPath(),
                SeatunnelApplicationConfig.load(
                        scenarioFile(scenario, "application.config").toPath(),
                        Collections.emptyMap()));
    }

    /**
     * Reads the logs of a Pod of a running application; fails when the application already ended.
     */
    protected static String livePodLogs(KubernetesApplicationClient application, String name)
            throws Exception {
        ApplicationStatus status = application.getStatus();
        if (status.isTerminal() || status == ApplicationStatus.UNKNOWN) {
            throw new IllegalStateException(
                    "Application stopped while waiting for Pod " + name + " logs: " + status);
        }
        return retainedPodLogs(name);
    }

    /** Reads retained Pod logs even after the application has finished, for failure diagnostics. */
    protected static String retainedPodLogs(String name) throws Exception {
        try {
            String logs =
                    core.readNamespacedPodLog(
                            name,
                            namespace,
                            CONTAINER_NAME,
                            false,
                            false,
                            null,
                            null,
                            false,
                            null,
                            null,
                            false);
            // The SDK returns null for an empty successful response while logs are starting.
            return logs == null ? "" : logs;
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

    /** Brings up the disposable cluster once per JVM and registers its cleanup. */
    private static synchronized void startCluster() throws Exception {
        if (started) {
            return;
        }
        started = true;
        try {
            Map<String, String> options =
                    SeatunnelApplicationConfig.load(
                            suiteFile("suite.config").toPath(), Collections.emptyMap());
            namespace = options.get(KubernetesOptions.NAMESPACE.key());
            kubeconfig = Paths.get(options.get(KubernetesOptions.KUBE_CONFIG.key()));
            String image = options.get(KubernetesOptions.IMAGE.key());
            buildApplicationImage(image);
            startK3s();
            importImage(image);
            releaseSystemCpuRequests();
            connectKubernetes();
            if (MULTI_NODE) {
                Awaitility.await()
                        .atMost(180, TimeUnit.SECONDS)
                        .untilAsserted(
                                () -> {
                                    List<io.kubernetes.client.openapi.models.V1Node> nodes =
                                            core.listNode(
                                                            null, null, null, null, null, null,
                                                            null, null, null, null, null)
                                                    .getItems();
                                    assertEquals(3, nodes.size());
                                    assertTrue(
                                            nodes.stream()
                                                    .allMatch(
                                                            node ->
                                                                    node.getStatus().getConditions()
                                                                            .stream()
                                                                            .anyMatch(
                                                                                    condition ->
                                                                                            "Ready"
                                                                                                            .equals(
                                                                                                                    condition
                                                                                                                            .getType())
                                                                                                    && "True"
                                                                                                            .equals(
                                                                                                                    condition
                                                                                                                            .getStatus()))));
                                });
                pinFixtureDnsToControlNode();
                startStorage();
            }
            createNamespace();
            createSharedFixtures();
            deployer = new KubernetesApplicationClusterDescriptorFactory().create(options);
            platformMonitor = KubernetesClientFactory.create(options, false);
        } catch (Exception | AssertionError failure) {
            stopCluster();
            throw failure;
        }
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                AbstractKubernetesApplicationIT::stopCluster,
                                "kubernetes-application-e2e-cleanup"));
    }

    /** Starts the K3s container and writes the kubeconfig the submitting client uses. */
    private static void startK3s() throws Exception {
        k3s =
                new K3sContainer(DockerImageName.parse(K3S_IMAGE))
                        .withStartupTimeout(Duration.ofMinutes(3))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(K3S_IMAGE)));
        if (MULTI_NODE) {
            clusterNetwork = Network.newNetwork();
            k3s.withNetwork(clusterNetwork)
                    .withNetworkAliases("ha-server")
                    .withEnv("K3S_TOKEN", "local-ha-fixture-token")
                    .withCreateContainerCmdModifier(
                            command -> command.withEntrypoint("/bin/sh", "-c"))
                    .withCommand(nodeCommand("server", "ha-server", "control"));
        }
        k3s.start();
        if (MULTI_NODE) {
            for (int index = 1; index <= 2; index++) {
                String name = "ha-node-" + index;
                GenericContainer<?> node =
                        new GenericContainer<>(DockerImageName.parse(K3S_IMAGE))
                                .withNetwork(clusterNetwork)
                                .withNetworkAliases(name)
                                .withPrivilegedMode(true)
                                .withCreateContainerCmdModifier(
                                        command -> {
                                            command.withEntrypoint("/bin/sh", "-c");
                                            command.getHostConfig().withCgroupnsMode("host");
                                        })
                                .withFileSystemBind(
                                        "/sys/fs/cgroup", "/sys/fs/cgroup", BindMode.READ_WRITE)
                                .withTmpFs(Map.of("/run", "", "/var/run", ""))
                                .withEnv("K3S_URL", "https://ha-server:6443")
                                .withEnv("K3S_TOKEN", "local-ha-fixture-token")
                                .withCommand(nodeCommand("agent", name, "compute"))
                                .waitingFor(
                                        Wait.forSuccessfulCommand(
                                                "test -S /run/k3s/containerd/containerd.sock"))
                                .withStartupTimeout(Duration.ofMinutes(3));
                computeNodes.put(name, node);
                node.start();
            }
        }
        Files.createDirectories(kubeconfig.getParent());
        Files.write(kubeconfig, k3s.getKubeConfigYaml().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Frees CPU reservations of the cluster's system deployments. The isolation case runs two
     * masters and two workers (four CPUs and 3 GiB in total), which has to fit the standard public
     * GitHub Actions runner; actual CPU use stays shared and memory reservations are unchanged.
     */
    private static void releaseSystemCpuRequests() throws Exception {
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
    }

    /** Builds the Kubernetes clients and fails when the freshly started cluster is unreachable. */
    private static void connectKubernetes() throws Exception {
        apiClient = Config.fromConfig(kubeconfig.toString());
        apiClient.setReadTimeout(30000);
        core = new CoreV1Api(apiClient);
        batch = new BatchV1Api(apiClient);
        core.listNamespace(null, null, null, null, null, null, null, null, null, 10, null);
    }

    private static void createNamespace() throws Exception {
        core.createNamespace(
                new V1Namespace().metadata(new V1ObjectMeta().name(namespace)),
                null,
                null,
                null,
                null);
        namespaceCreated = true;
    }

    /** Removes the disposable cluster; also invoked by the JVM shutdown hook after the suite. */
    private static synchronized void stopCluster() {
        if (!started) {
            return;
        }
        started = false;
        try {
            if (deployer != null) {
                deployer.close();
            }
        } catch (Exception failure) {
            LOG.warn("Cannot close the application deployer", failure);
        } finally {
            try {
                if (namespaceCreated && core != null) {
                    core.deleteNamespace(namespace, null, null, 0, null, "Foreground", null);
                }
            } catch (Exception failure) {
                LOG.warn("Cannot delete the test namespace", failure);
            } finally {
                try {
                    if (platformMonitor != null) {
                        platformMonitor.close();
                    }
                } catch (Exception failure) {
                    LOG.warn("Cannot close the platform monitor", failure);
                }
                if (apiClient != null) {
                    apiClient.getHttpClient().dispatcher().executorService().shutdown();
                    apiClient.getHttpClient().connectionPool().evictAll();
                }
                for (Map.Entry<String, GenericContainer<?>> entry : computeNodes.entrySet()) {
                    GenericContainer<?> node = entry.getValue();
                    try {
                        if (unavailableNodes.contains(entry.getKey())) {
                            pauseComputeNode(entry.getKey(), false);
                        }
                        if (Boolean.TRUE.equals(
                                docker().inspectContainerCmd(node.getContainerId())
                                        .exec()
                                        .getState()
                                        .getPaused())) {
                            docker().unpauseContainerCmd(node.getContainerId()).exec();
                        }
                    } catch (Exception | AssertionError failure) {
                        LOG.warn("Cannot restore a disposable compute node", failure);
                    } finally {
                        try {
                            node.close();
                        } catch (Exception failure) {
                            LOG.warn("Cannot close a disposable compute node", failure);
                        }
                    }
                }
                computeNodes.clear();
                if (storage != null) {
                    try {
                        storageAvailable(true);
                    } catch (Exception failure) {
                        LOG.warn("Cannot restore disposable storage", failure);
                    } finally {
                        try {
                            storage.close();
                        } catch (Exception failure) {
                            LOG.warn("Cannot close disposable storage", failure);
                        }
                    }
                }
                if (k3s != null) {
                    try {
                        k3s.close();
                    } catch (Exception failure) {
                        LOG.warn("Cannot close disposable control node", failure);
                    }
                }
                if (MULTI_NODE) {
                    cleanNodeCgroups();
                }
                if (clusterNetwork != null) {
                    clusterNetwork.close();
                }
                try {
                    if (kubeconfig != null) {
                        Files.deleteIfExists(kubeconfig);
                    }
                } catch (Exception failure) {
                    LOG.warn("Cannot delete the generated kubeconfig", failure);
                }
            }
        }
    }

    /**
     * Creates the suite-wide fixtures every scenario class shares: the runtime ConfigMap, the
     * application ServiceAccount and its RBAC Role/RoleBinding. Class-scoped fixtures belong in
     * {@link #startScenarioFixtures()}.
     */
    private static void createSharedFixtures() throws Exception {
        core.createNamespacedServiceAccount(
                namespace,
                Yaml.loadAs(suiteFile("service-account.yaml"), V1ServiceAccount.class),
                null,
                null,
                null,
                null);
        Map<String, String> runtimeConfiguration = new HashMap<>();
        runtimeConfiguration.put(
                "seatunnel.yaml",
                new String(
                        Files.readAllBytes(suiteFile("seatunnel.yaml").toPath()),
                        StandardCharsets.UTF_8));
        if (MULTI_NODE) {
            runtimeConfiguration.put(
                    "seatunnel.yaml",
                    runtimeConfiguration
                            .get("seatunnel.yaml")
                            .replace("storage.type: hdfs", "storage.type: s3")
                            .replace(
                                    "fs.defaultFS: file:///",
                                    "s3.bucket: s3a://ha-checkpoints\n          fs.s3a.endpoint: "
                                            + storageEndpoint
                                            + "\n          fs.s3a.path.style.access: true\n          fs.s3a.connection.ssl.enabled: false"
                                            + "\n          fs.s3a.access.key: fixture-access\n          fs.s3a.secret.key: fixture-secret"
                                            + "\n          fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
                                            + "\n          fs.s3a.attempts.maximum: 1\n          fs.s3a.connection.establish.timeout: 1000\n          fs.s3a.connection.timeout: 2000")
                            .replace(
                                    "namespace: /opt/seatunnel/checkpoints/",
                                    "namespace: /ha-checkpoints/"));
        }
        runtimeConfiguration.put(
                "log4j2_client.properties", distributionConfig("log4j2_client.properties"));
        // No hardcoded runtime files: take them from the distribution configuration.
        for (String name :
                Arrays.asList(
                        "hazelcast-master.yaml",
                        "hazelcast-worker.yaml",
                        "jvm_options",
                        "jvm_master_options",
                        "jvm_worker_options")) {
            runtimeConfiguration.put(name, distributionConfig(name));
        }
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
                Yaml.loadAs(suiteFile("role.yaml"), V1Role.class),
                null,
                null,
                null,
                null);
        V1RoleBinding roleBinding =
                Yaml.loadAs(suiteFile("role-binding.yaml"), V1RoleBinding.class);
        rbac.createNamespacedRoleBinding(namespace, roleBinding, null, null, null, null);
    }

    /**
     * Adds or replaces one entry of the shared runtime ConfigMap. Scenario classes that need extra
     * runtime configuration change it through this method and {@link #removeRuntimeConfigMapEntry}.
     */
    protected static void putRuntimeConfigMapEntry(String key, String value) throws Exception {
        V1ConfigMap runtime = core.readNamespacedConfigMap(RUNTIME_CONFIG_MAP, namespace, null);
        runtime.getData().put(key, value);
        core.replaceNamespacedConfigMap(
                RUNTIME_CONFIG_MAP, namespace, runtime, null, null, null, null);
    }

    /** Removes one entry that {@link #putRuntimeConfigMapEntry} added. */
    protected static void removeRuntimeConfigMapEntry(String key) throws Exception {
        V1ConfigMap runtime = core.readNamespacedConfigMap(RUNTIME_CONFIG_MAP, namespace, null);
        runtime.getData().remove(key);
        core.replaceNamespacedConfigMap(
                RUNTIME_CONFIG_MAP, namespace, runtime, null, null, null, null);
    }

    /** Reads one file of the distribution {@code config/} directory. */
    private static String distributionConfig(String name) throws IOException {
        return new String(
                Files.readAllBytes(Paths.get(PROJECT_ROOT_PATH, "config", name)),
                StandardCharsets.UTF_8);
    }

    private static void importImage(String image) throws Exception {
        LOG.info("Importing application image {} into disposable K3s", image);
        Path archive = Files.createTempFile("seatunnel-k3s-image-", ".tar");
        try {
            try (InputStream input = docker().saveImageCmd(image).exec()) {
                Files.copy(input, archive, StandardCopyOption.REPLACE_EXISTING);
            }
            List<GenericContainer<?>> nodes = new ArrayList<>();
            nodes.add(k3s);
            nodes.addAll(computeNodes.values());
            java.util.concurrent.ExecutorService imports =
                    java.util.concurrent.Executors.newFixedThreadPool(nodes.size());
            try {
                List<java.util.concurrent.CompletableFuture<Void>> pending = new ArrayList<>();
                for (GenericContainer<?> node : nodes) {
                    pending.add(
                            java.util.concurrent.CompletableFuture.runAsync(
                                    () -> {
                                        try {
                                            node.copyFileToContainer(
                                                    MountableFile.forHostPath(archive),
                                                    "/tmp/seatunnel-application-image.tar");
                                            Container.ExecResult imported =
                                                    node.execInContainer(
                                                            "ctr",
                                                            "--address",
                                                            CONTAINERD_SOCKET,
                                                            "--namespace",
                                                            "k8s.io",
                                                            "images",
                                                            "import",
                                                            "/tmp/seatunnel-application-image.tar");
                                            assertEquals(
                                                    0,
                                                    imported.getExitCode(),
                                                    imported.getStderr());
                                            node.execInContainer(
                                                    "rm", "/tmp/seatunnel-application-image.tar");
                                        } catch (Exception failure) {
                                            throw new java.util.concurrent.CompletionException(
                                                    failure);
                                        }
                                    },
                                    imports));
                }
                java.util.concurrent.CompletableFuture.allOf(
                                pending.toArray(new java.util.concurrent.CompletableFuture<?>[0]))
                        .get();
            } finally {
                imports.shutdownNow();
            }

        } finally {
            Files.deleteIfExists(archive);
        }
    }

    private static void buildApplicationImage(String image) throws Exception {
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

            if (MULTI_NODE) {
                Files.copy(
                        DependencyJar.staged("seatunnel-shade-hadoop-aws.jar").path(),
                        lib.resolve("seatunnel-shade-hadoop-aws.jar"));
            }
            for (String connector : Arrays.asList("fake", "console", "assert", "file-local")) {
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
            // Ship only the tiny gate fixture and its existing checkpointable split protocol.
            // No production connector or worker checkpoint-volume behavior changes.
            try (java.util.jar.JarOutputStream jar =
                    new java.util.jar.JarOutputStream(
                            Files.newOutputStream(
                                    connectors.resolve("connector-kubernetes-gated.jar")))) {
                java.util.List<Class<?>> classes = new java.util.ArrayList<>();
                classes.add(KubernetesGatedSequenceSource.class);
                classes.addAll(
                        Arrays.asList(KubernetesGatedSequenceSource.class.getDeclaredClasses()));
                for (String name :
                        Arrays.asList(
                                "CheckpointableSequenceSource",
                                "CheckpointableSequenceSourceFactory",
                                "CheckpointableSequenceSourceReader",
                                "CheckpointableSequenceSplit",
                                "CheckpointableSequenceSplitEnumerator",
                                "CheckpointableSequenceState")) {
                    classes.add(
                            Class.forName(
                                    "org.apache.seatunnel.e2e.source.checkpointable." + name));
                }
                for (Class<?> type : classes) {
                    String resource = type.getName().replace('.', '/') + ".class";
                    jar.putNextEntry(new java.util.jar.JarEntry(resource));
                    try (InputStream input = type.getClassLoader().getResourceAsStream(resource)) {
                        if (input == null)
                            throw new IOException("Missing test fixture " + resource);
                        input.transferTo(jar);
                    }
                    jar.closeEntry();
                }
                jar.putNextEntry(
                        new java.util.jar.JarEntry(
                                "META-INF/services/org.apache.seatunnel.api.table.factory.Factory"));
                jar.write(
                        (KubernetesGatedSequenceSource.Factory.class.getName() + "\n")
                                .getBytes(StandardCharsets.UTF_8));
                jar.closeEntry();
            }
            Files.writeString(
                    connectors.resolve("plugin-mapping.properties"),
                    "\nseatunnel.source.KubernetesGatedSequence = connector-kubernetes-gated\n",
                    java.nio.file.StandardOpenOption.APPEND);
            String probeClass =
                    KubernetesApplicationProbe.class.getName().replace('.', '/') + ".class";
            Path probe = home.resolve("test-probe").resolve(probeClass);
            Files.createDirectories(probe.getParent());
            try (InputStream input =
                    KubernetesApplicationProbe.class
                            .getClassLoader()
                            .getResourceAsStream(probeClass)) {
                if (input == null) {
                    throw new IOException("Missing compiled Kubernetes test probe");
                }
                Files.copy(input, probe);
            }
            Files.copy(suiteFile("Dockerfile").toPath(), context.resolve("Dockerfile"));
            docker().buildImageCmd(context.toFile())
                    .withBuildArg("JAVA_VERSION", String.valueOf(Runtime.version().feature()))
                    .withTags(Collections.singleton(image))
                    .start()
                    .awaitImageId();
        } finally {
            FileUtils.deleteDirectory(context.toFile());
        }
    }

    private static DockerClient docker() {
        return DockerClientFactory.lazyClient();
    }

    private static String[] nodeCommand(String mode, String name, String label) {
        String root = CGROUP_PREFIX + "-" + name;
        return new String[] {
            "mkdir -p /sys/fs/cgroup/"
                    + root
                    + " /sys/fs/cgroup/"
                    + root
                    + "-system"
                    + " && exec /bin/k3s "
                    + mode
                    // Nodes share a private Docker bridge. Use direct kubelet connections so a
                    // frozen node's stale management tunnel cannot poison later fault scenarios.
                    + ("server".equals(mode)
                            ? " --disable=traefik --disable=servicelb --egress-selector-mode=disabled"
                            : "")
                    + " --node-name="
                    + name
                    + " --node-label=seatunnel.e2e/"
                    + label
                    + "=true"
                    + " --kubelet-arg=cgroup-root=/"
                    + root
                    + " --kubelet-arg=kubelet-cgroups=/"
                    + root
                    + "-system"
                    + " --kubelet-arg=runtime-cgroups=/"
                    + root
                    + "-system"
        };
    }

    private static void pinFixtureDnsToControlNode() throws Exception {
        Container.ExecResult patched =
                k3s.execInContainer(
                        "kubectl",
                        "-n",
                        "kube-system",
                        "patch",
                        "deployment",
                        "coredns",
                        "--type=merge",
                        "-p",
                        "{\"spec\":{\"template\":{\"spec\":{\"nodeSelector\":{\"seatunnel.e2e/control\":\"true\"}}}}}");
        assertEquals(0, patched.getExitCode(), patched.getStderr());
        Container.ExecResult ready =
                k3s.execInContainer(
                        "kubectl",
                        "-n",
                        "kube-system",
                        "rollout",
                        "status",
                        "deployment/coredns",
                        "--timeout=120s");
        assertEquals(0, ready.getExitCode(), ready.getStderr());
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Container.ExecResult nodes =
                                    k3s.execInContainer(
                                            "kubectl",
                                            "-n",
                                            "kube-system",
                                            "get",
                                            "pods",
                                            "-l",
                                            "k8s-app=kube-dns",
                                            "-o",
                                            "jsonpath={range .items[*]}{.spec.nodeName}{'\\n'}{end}");
                            assertEquals(0, nodes.getExitCode(), nodes.getStderr());
                            List<String> observed =
                                    nodes.getStdout()
                                            .lines()
                                            .filter(node -> !node.isBlank())
                                            .collect(java.util.stream.Collectors.toList());
                            assertFalse(observed.isEmpty());
                            assertTrue(observed.stream().allMatch("ha-server"::equals));
                        });
    }

    private static void startStorage() {
        storage =
                new GenericContainer<>(DockerImageName.parse(STORAGE_IMAGE))
                        .withNetwork(clusterNetwork)
                        .withNetworkAliases("ha-storage")
                        .withEnv("MINIO_ROOT_USER", "fixture-access")
                        .withEnv("MINIO_ROOT_PASSWORD", "fixture-secret")
                        .withExposedPorts(9000)
                        .withCreateContainerCmdModifier(
                                command ->
                                        command.getHostConfig()
                                                .withPortBindings(
                                                        new com.github.dockerjava.api.model
                                                                .PortBinding(
                                                                com.github.dockerjava.api.model
                                                                        .Ports.Binding
                                                                        .bindIpAndPort(
                                                                                "127.0.0.1", 0),
                                                                new com.github.dockerjava.api.model
                                                                        .ExposedPort(9000))))
                        .withCommand("server", "/data")
                        .waitingFor(Wait.forHttp("/minio/health/ready").forPort(9000));
        storage.start();
        storageEndpoint =
                "http://"
                        + storage.getContainerInfo()
                                .getNetworkSettings()
                                .getNetworks()
                                .values()
                                .iterator()
                                .next()
                                .getIpAddress()
                        + ":9000";
        com.amazonaws.services.s3.AmazonS3 client = storageClient();
        try {
            client.createBucket("ha-checkpoints");
            client.putObject("ha-checkpoints", "user-sentinel", "preserve-user-object");
        } finally {
            client.shutdown();
        }
    }

    protected static com.amazonaws.services.s3.AmazonS3 storageClient() {
        return com.amazonaws.services.s3.AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(
                        new com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration(
                                "http://" + storage.getHost() + ":" + storage.getMappedPort(9000),
                                "us-east-1"))
                .withClientConfiguration(
                        new com.amazonaws.ClientConfiguration()
                                .withConnectionTimeout(2000)
                                .withSocketTimeout(2000)
                                .withMaxErrorRetry(0))
                .withCredentials(
                        new com.amazonaws.auth.AWSStaticCredentialsProvider(
                                new com.amazonaws.auth.BasicAWSCredentials(
                                        "fixture-access", "fixture-secret")))
                .withPathStyleAccessEnabled(true)
                .build();
    }

    protected static long storedCheckpoint(long jobId) throws Exception {
        com.amazonaws.services.s3.AmazonS3 client = storageClient();
        try {
            long latest = -1;
            for (com.amazonaws.services.s3.model.S3ObjectSummary object :
                    client.listObjectsV2("ha-checkpoints", "ha-checkpoints/" + jobId + "/")
                            .getObjectSummaries()) {
                if (!object.getKey().endsWith(".ser")) {
                    continue;
                }
                try (com.amazonaws.services.s3.model.S3Object stored =
                        client.getObject("ha-checkpoints", object.getKey())) {
                    org.apache.seatunnel.engine.checkpoint.storage.PipelineState state =
                            new org.apache.seatunnel.engine.serializer.protobuf
                                            .ProtoStuffSerializer()
                                    .deserialize(
                                            stored.getObjectContent().readAllBytes(),
                                            org.apache.seatunnel.engine.checkpoint.storage
                                                    .PipelineState.class);
                    assertEquals(Long.toString(jobId), state.getJobId());
                    latest = Math.max(latest, state.getCheckpointId());
                } catch (com.amazonaws.services.s3.model.AmazonS3Exception failure) {
                    // Native retention may remove an older listed checkpoint before this read.
                    if (failure.getStatusCode() != 404
                            || !"NoSuchKey".equals(failure.getErrorCode())) {
                        throw failure;
                    }
                }
            }
            assertTrue(latest >= 0, "A readable final checkpoint must exist in remote storage");
            assertEquals(
                    "preserve-user-object",
                    client.getObjectAsString("ha-checkpoints", "user-sentinel"));
            return latest;
        } finally {
            client.shutdown();
        }
    }

    protected static void storageAvailable(boolean available) {
        boolean paused =
                Boolean.TRUE.equals(
                        docker().inspectContainerCmd(storage.getContainerId())
                                .exec()
                                .getState()
                                .getPaused());
        if (available && paused) {
            docker().unpauseContainerCmd(storage.getContainerId()).exec();
        } else if (!available && !paused) {
            docker().pauseContainerCmd(storage.getContainerId()).exec();
        }
    }

    protected static void pauseComputeNode(String name, boolean paused) throws Exception {
        GenericContainer<?> node = computeNodes.get(name);
        assertTrue(node != null, "Fault must target a disposable compute node: " + name);
        String root = "/sys/fs/cgroup/" + CGROUP_PREFIX + "-" + name;
        if (paused) {
            for (String group : new String[] {root, root + "-system"}) {
                Container.ExecResult state = node.execInContainer("cat", group + "/cgroup.events");
                assertEquals(0, state.getExitCode(), state.getStderr());
                assertTrue(
                        state.getStdout().lines().anyMatch("populated 1"::equals),
                        group + " must contain the test node's processes before freezing");
            }
            unavailableNodes.add(name);
            Container.ExecResult result =
                    node.execInContainer(
                            "sh",
                            "-c",
                            "echo 1 > "
                                    + root
                                    + "/cgroup.freeze && echo 1 > "
                                    + root
                                    + "-system/cgroup.freeze");
            assertEquals(0, result.getExitCode(), result.getStderr());
            // Kubelet and Pod cgroups are outside Docker's own freezer. Freeze those explicitly
            // as well as the node's init/runtime, so the fault includes every application process.
            awaitNodeCgroupState(node, root, true);
            docker().pauseContainerCmd(node.getContainerId()).exec();
        } else {
            if (Boolean.TRUE.equals(
                    docker().inspectContainerCmd(node.getContainerId())
                            .exec()
                            .getState()
                            .getPaused())) {
                docker().unpauseContainerCmd(node.getContainerId()).exec();
            }
            Container.ExecResult result =
                    node.execInContainer(
                            "sh",
                            "-c",
                            "echo 0 > "
                                    + root
                                    + "-system/cgroup.freeze && echo 0 > "
                                    + root
                                    + "/cgroup.freeze");
            assertEquals(0, result.getExitCode(), result.getStderr());
            awaitNodeCgroupState(node, root, false);
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Container.ExecResult ready =
                                        k3s.execInContainer(
                                                "kubectl",
                                                "wait",
                                                "--for=condition=Ready",
                                                "node/" + name,
                                                "--timeout=5s");
                                assertEquals(0, ready.getExitCode(), ready.getStderr());
                                Container.ExecResult health =
                                        k3s.execInContainer(
                                                "kubectl",
                                                "get",
                                                "--request-timeout=5s",
                                                "--raw",
                                                "/api/v1/nodes/" + name + "/proxy/healthz");
                                assertEquals(0, health.getExitCode(), health.getStderr());
                                assertEquals("ok", health.getStdout().trim());
                            });
            unavailableNodes.remove(name);
        }
    }

    private static void awaitNodeCgroupState(
            GenericContainer<?> node, String root, boolean frozen) {
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            for (String group : new String[] {root, root + "-system"}) {
                                Container.ExecResult state =
                                        node.execInContainer("cat", group + "/cgroup.events");
                                assertEquals(0, state.getExitCode(), state.getStderr());
                                assertTrue(
                                        state.getStdout()
                                                .lines()
                                                .anyMatch(
                                                        line ->
                                                                line.equals(
                                                                        frozen
                                                                                ? "frozen 1"
                                                                                : "frozen 0")),
                                        group + " did not reach the requested freezer state");
                            }
                        });
    }
    /** Reconciles only this JVM's UUID-scoped cgroups after all fixture nodes have stopped. */
    private static void cleanNodeCgroups() {
        try (GenericContainer<?> cleanup =
                new GenericContainer<>(DockerImageName.parse(K3S_IMAGE))
                        .withPrivilegedMode(true)
                        .withCreateContainerCmdModifier(
                                command -> {
                                    command.withEntrypoint("/bin/sh");
                                    command.getHostConfig().withCgroupnsMode("host");
                                })
                        .withFileSystemBind("/sys/fs/cgroup", "/sys/fs/cgroup", BindMode.READ_WRITE)
                        .withCommand("-c", "sleep 300")) {
            cleanup.start();
            for (String name : Arrays.asList("ha-server", "ha-node-1", "ha-node-2")) {
                for (String suffix : Arrays.asList("", "-system")) {
                    String root = "/sys/fs/cgroup/" + CGROUP_PREFIX + "-" + name + suffix;
                    // Never traverse outside the exact roots created by nodeCommand.
                    Container.ExecResult result =
                            cleanup.execInContainer(
                                    "sh",
                                    "-c",
                                    "if [ -d "
                                            + root
                                            + " ]; then echo 0 > "
                                            + root
                                            + "/cgroup.freeze; "
                                            + "echo 1 > "
                                            + root
                                            + "/cgroup.kill; "
                                            + "sleep 1; find "
                                            + root
                                            + " -depth -type d -exec rmdir {} +; fi; test ! -d "
                                            + root);
                    assertEquals(0, result.getExitCode(), result.getStderr());
                }
            }
            LOG.info("Removed all exact fixture node cgroups {}", CGROUP_PREFIX);
        } catch (Exception | AssertionError failure) {
            LOG.error("Cannot remove exact fixture node cgroups {}", CGROUP_PREFIX, failure);
        }
    }
}
