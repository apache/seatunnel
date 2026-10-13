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
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;

import org.codehaus.plexus.util.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
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
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Role;
import io.kubernetes.client.openapi.models.V1RoleBinding;
import io.kubernetes.client.openapi.models.V1ServiceAccount;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Yaml;

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
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.getResourcesFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
public abstract class AbstractKubernetesApplicationIT implements TestResource {
    private static final String K3S_IMAGE = "rancher/k3s:v1.31.6-k3s1";
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
    private static KubernetesClient platformMonitor;
    private static boolean namespaceCreated;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        startCluster();
        startScenarioFixtures();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        stopScenarioFixtures();
    }

    /**
     * Scenario classes override to create the class-scoped fixtures their tests need. The shared
     * cluster is already available when this hook runs; the base environment stays generic and the
     * cluster itself is removed by its JVM shutdown hook after the suite.
     */
    protected void startScenarioFixtures() throws Exception {}

    /** Scenario classes override to release what {@link #startScenarioFixtures()} created. */
    protected void stopScenarioFixtures() throws Exception {}

    protected static KubernetesApplicationClient deployApplication(String scenario)
            throws Exception {
        return deployApplication(specification(scenario), scenario);
    }

    protected static KubernetesApplicationClient deployApplication(
            ApplicationSpecification specification, String scenario) throws Exception {
        Map<String, String> options =
                SeatunnelApplicationConfig.load(
                        getResourcesFile("/kubernetes/" + scenario + "/application.config")
                                .toPath(),
                        Collections.emptyMap());
        ApplicationClusterDeployer applicationDeployer =
                new ApplicationClusterDeployer(DeployType.KUBERNETES, specification, options);
        return new KubernetesApplicationClient(platformMonitor, applicationDeployer.run());
    }

    protected static void awaitStatus(
            KubernetesApplicationClient application, ApplicationStatus expected) {
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

    protected static void recordPodDiagnostics(KubernetesApplicationClient application) {
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

    protected static void awaitWorkersRunning(
            KubernetesApplicationClient application, int expected) {
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

    protected static void awaitWorkersRemoved(KubernetesApplicationClient application) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> assertTrue(workers(application).isEmpty()));
    }

    /** Kills the container of one application Pod inside the disposable cluster. */
    protected static void killPodContainer(V1Pod pod) throws Exception {
        String containerId = pod.getStatus().getContainerStatuses().get(0).getContainerID();
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
                        containerId.substring("containerd://".length()));
        assertEquals(0, result.getExitCode(), result.getStderr());
    }

    protected static String inlineSpecification(V1Pod master) {
        return master.getSpec().getContainers().get(0).getEnv().stream()
                .filter(env -> KubernetesConstants.SPECIFICATION_ENV.equals(env.getName()))
                .map(V1EnvVar::getValue)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing inline specification"));
    }

    protected static V1EnvVar environment(V1Pod pod, String name) {
        return pod.getSpec().getContainers().get(0).getEnv().stream()
                .filter(env -> name.equals(env.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing environment variable " + name));
    }

    protected static boolean envFromSecret(V1Pod pod, String secretName) {
        return pod.getSpec().getContainers().get(0).getEnvFrom().stream()
                .anyMatch(
                        source ->
                                source.getSecretRef() != null
                                        && secretName.equals(source.getSecretRef().getName()));
    }

    protected static boolean envFromConfigMap(V1Pod pod, String configMapName) {
        return pod.getSpec().getContainers().get(0).getEnvFrom().stream()
                .anyMatch(
                        source ->
                                source.getConfigMapRef() != null
                                        && configMapName.equals(
                                                source.getConfigMapRef().getName()));
    }

    protected static boolean mounted(V1Pod pod, String path) {
        return pod.getSpec().getContainers().get(0).getVolumeMounts().stream()
                .anyMatch(
                        mount ->
                                path.equals(mount.getMountPath())
                                        && Boolean.TRUE.equals(mount.getReadOnly()));
    }

    protected static void awaitAllResourcesRemoved(KubernetesApplicationClient application) {
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

    protected static List<V1Pod> pods(KubernetesApplicationClient application, String role)
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
                getResourcesFile("/kubernetes/" + scenario + "/job.conf").toPath(),
                SeatunnelApplicationConfig.load(
                        getResourcesFile("/kubernetes/" + scenario + "/application.config")
                                .toPath(),
                        Collections.emptyMap()));
    }

    protected static String podLogs(KubernetesApplicationClient application, String name)
            throws Exception {
        ApplicationStatus status = application.getStatus();
        if (status.isTerminal() || status == ApplicationStatus.UNKNOWN) {
            throw new IllegalStateException(
                    "Application stopped while waiting for Pod " + name + " logs: " + status);
        }
        return podLogs(name);
    }

    /** Reads retained Pod logs even after the application has finished, for failure diagnostics. */
    protected static String podLogs(String name) throws Exception {
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

    /** Builds the application image and starts the disposable cluster on first use. */
    private static synchronized void startCluster() throws Exception {
        if (started) {
            return;
        }
        started = true;
        try {
            Path applicationConfig =
                    getResourcesFile("/kubernetes/batch/application.config").toPath();
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
            createFixtures(options);
        } catch (Exception failure) {
            stopCluster();
            throw failure;
        }
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                AbstractKubernetesApplicationIT::stopCluster,
                                "kubernetes-application-e2e-cleanup"));
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
                if (k3s != null) {
                    k3s.close();
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

    private static void createFixtures(Map<String, String> options) throws Exception {
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
        // No hardcoded runtime files: take them from the distribution configuration.
        for (String name :
                Arrays.asList(
                        "hazelcast-master.yaml",
                        "hazelcast-worker.yaml",
                        "jvm_options",
                        "jvm_master_options",
                        "jvm_worker_options")) {
            runtimeConfiguration.put(
                    name,
                    new String(
                            Files.readAllBytes(Paths.get(PROJECT_ROOT_PATH, "config", name)),
                            StandardCharsets.UTF_8));
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

    private static void importImage(String image) throws Exception {
        LOG.info("Importing application image {} into disposable K3s", image);
        Path archive = Files.createTempFile("seatunnel-k3s-image-", ".tar");
        try {
            try (InputStream input = docker().saveImageCmd(image).exec()) {
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
            docker().buildImageCmd(context.toFile())
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
}
