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
import org.apache.seatunnel.engine.client.deployment.ClusterDescriptor;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;

import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Pod;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.getResourcesFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test-facing helpers for the application-mode integration scenarios.
 *
 * <p>The disposable cluster and its fixtures belong to {@link KubernetesApplicationTestResource},
 * which every scenario class shares. This base only exposes read access to that environment plus
 * the polling, inspection and assertion helpers the scenarios use.
 */
@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK, EngineType.SPARK, EngineType.SEATUNNEL})
public abstract class AbstractKubernetesApplicationIT extends TestSuiteBase {
    protected static final String APPLICATION_LABEL = "seatunnel.apache.org/application-id";
    protected static final String ROLE_LABEL = "seatunnel.apache.org/role";
    protected static final String RUNTIME_CONFIG_MAP = "seatunnel-runtime-configuration";
    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractKubernetesApplicationIT.class);

    private static KubernetesApplicationTestResource resource;

    @BeforeAll
    void prepareApplicationEnvironment() throws Exception {
        resource = KubernetesApplicationTestResource.shared(RUNTIME_CONFIG_MAP);
    }

    protected static String namespace() {
        return resource.namespace();
    }

    protected static CoreV1Api core() {
        return resource.core();
    }

    protected static BatchV1Api batch() {
        return resource.batch();
    }

    protected static ApiClient apiClient() {
        return resource.apiClient();
    }

    protected static ClusterDescriptor<String> deployer() {
        return resource.deployer();
    }

    protected static KubernetesApplicationClient deployApplication(String scenario)
            throws Exception {
        return deployApplication(specification(scenario), scenario);
    }

    protected static KubernetesApplicationClient deployApplication(
            ApplicationSpecification specification, String scenario) throws Exception {
        return resource.deploy(specification, scenario);
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
                                            deployer()
                                                    .getApplicationStatus(
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
                            core().readNamespacedPodLog(
                                            name,
                                            namespace(),
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
        resource.killContainer(pod.getStatus().getContainerStatuses().get(0).getContainerID());
    }

    protected static String inlineSpecification(V1Pod master) {
        return master.getSpec().getContainers().get(0).getEnv().stream()
                .filter(env -> KubernetesConstants.SPECIFICATION_ENV.equals(env.getName()))
                .map(env -> env.getValue())
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
                                                    batch().readNamespacedJob(
                                                                    application.getClusterId(),
                                                                    namespace(),
                                                                    null)));
                            assertTrue(
                                    missing(
                                            () ->
                                                    core().readNamespacedService(
                                                                    application.getClusterId(),
                                                                    namespace(),
                                                                    null)));
                            assertTrue(
                                    missing(
                                            () ->
                                                    core().readNamespacedSecret(
                                                                    application.getClusterId(),
                                                                    namespace(),
                                                                    null)));
                            assertTrue(
                                    missing(
                                            () ->
                                                    core().readNamespacedConfigMap(
                                                                    application.getClusterId(),
                                                                    namespace(),
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
        return core().listNamespacedPod(
                        namespace(),
                        null,
                        null,
                        null,
                        null,
                        selector,
                        null,
                        null,
                        null,
                        null,
                        null,
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
            return core().readNamespacedPodLog(
                            name,
                            namespace(),
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
}
