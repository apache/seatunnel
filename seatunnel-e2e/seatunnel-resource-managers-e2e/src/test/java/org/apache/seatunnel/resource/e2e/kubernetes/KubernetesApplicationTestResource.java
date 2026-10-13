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

import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.engine.client.deployment.ApplicationClusterDeployer;
import org.apache.seatunnel.engine.client.deployment.ClusterDescriptor;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.kubernetes.KubernetesApplicationClusterDescriptorFactory;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationClient;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClientFactory;

import org.codehaus.plexus.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.github.dockerjava.api.DockerClient;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
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
import java.util.Map;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.getResourcesFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Disposable K3s cluster, clients and shared fixtures for the application-mode integration tests.
 *
 * <p>One resource is started per test JVM and reused by every scenario class: the application image
 * is built and imported once, and the namespace, ServiceAccount, reference Secret, runtime
 * ConfigMap and RBAC objects are created once. {@link #shared(String)} registers a shutdown hook
 * that removes the namespace and stops the container after the suite.
 */
final class KubernetesApplicationTestResource implements AutoCloseable {
    private static final String K3S_IMAGE = "rancher/k3s:v1.31.6-k3s1";
    private static final Logger LOG =
            LoggerFactory.getLogger(KubernetesApplicationTestResource.class);
    private static KubernetesApplicationTestResource shared;

    /** Returns the JVM-wide resource, starting the cluster on first use. */
    static synchronized KubernetesApplicationTestResource shared(String runtimeConfigMap)
            throws Exception {
        if (shared == null) {
            KubernetesApplicationTestResource started = new KubernetesApplicationTestResource();
            try {
                started.start(runtimeConfigMap);
            } catch (Exception failure) {
                started.close();
                throw failure;
            }
            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(started::close, "kubernetes-application-e2e-cleanup"));
            shared = started;
        }
        return shared;
    }

    private String namespace;
    private CoreV1Api core;
    private BatchV1Api batch;
    private ApiClient apiClient;
    private K3sContainer k3s;
    private Path kubeconfig;
    private Path applicationConfig;
    private ClusterDescriptor<String> deployer;
    private KubernetesClient platformMonitor;
    private boolean namespaceCreated;

    private KubernetesApplicationTestResource() {}

    String namespace() {
        return namespace;
    }

    CoreV1Api core() {
        return core;
    }

    BatchV1Api batch() {
        return batch;
    }

    ApiClient apiClient() {
        return apiClient;
    }

    Path applicationConfig() {
        return applicationConfig;
    }

    ClusterDescriptor<String> deployer() {
        return deployer;
    }

    /** Submits one scenario through the platform deployer without connecting to its master. */
    KubernetesApplicationClient deploy(ApplicationSpecification specification, String scenario)
            throws Exception {
        Map<String, String> options =
                SeatunnelApplicationConfig.load(
                        getResourcesFile("/kubernetes/" + scenario + "/application.config")
                                .toPath(),
                        Collections.emptyMap());
        ApplicationClusterDeployer deployer =
                new ApplicationClusterDeployer(DeployType.KUBERNETES, specification, options);
        return new KubernetesApplicationClient(platformMonitor, deployer.run());
    }

    /** Kills one container of the disposable cluster by its runtime identifier. */
    void killContainer(String containerId) throws Exception {
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

    @Override
    public void close() {
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

    private void start(String runtimeConfigMap) throws Exception {
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
        createFixtures(runtimeConfigMap, options);
    }

    private void createFixtures(String runtimeConfigMap, Map<String, String> options)
            throws Exception {
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
                        .metadata(new V1ObjectMeta().name(runtimeConfigMap))
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

    private void importImage(String image) throws Exception {
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
