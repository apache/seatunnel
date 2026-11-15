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

import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationClient;

import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1Service;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies Secret, ConfigMap and job-configuration sources in a disposable K3s cluster. */
public class KubernetesApplicationConfigurationIT extends AbstractKubernetesApplicationIT {
    private static final String REFERENCE_SECRET = "seatunnel-application-e2e-ref-secret";

    /** Fixtures only the configuration scenarios need; the base environment stays generic. */
    @Override
    protected void startScenarioFixtures() throws Exception {
        core.createNamespacedSecret(
                namespace,
                new V1Secret()
                        .metadata(new V1ObjectMeta().name(REFERENCE_SECRET))
                        .type("Opaque")
                        .stringData(Collections.singletonMap("E2E_PARALLELISM", "1")),
                null,
                null,
                null,
                null);
        // Valid environment-variable name proving envFrom values resolve job placeholders.
        putRuntimeConfigMapEntry("E2E_CM_ENV", "configmap-env");
    }

    /** Restores the shared runtime ConfigMap and removes the class-scoped reference Secret. */
    @Override
    protected void stopScenarioFixtures() throws Exception {
        removeRuntimeConfigMapEntry("E2E_CM_ENV");
        core.deleteNamespacedSecret(
                REFERENCE_SECRET, namespace, null, null, null, null, null, null);
    }

    private static V1EnvVar environment(V1Pod pod, String name) {
        return pod.getSpec().getContainers().get(0).getEnv().stream()
                .filter(env -> name.equals(env.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing environment variable " + name));
    }

    private static boolean envFromSecret(V1Pod pod, String secretName) {
        return pod.getSpec().getContainers().get(0).getEnvFrom().stream()
                .anyMatch(
                        source ->
                                source.getSecretRef() != null
                                        && secretName.equals(source.getSecretRef().getName()));
    }

    private static boolean envFromConfigMap(V1Pod pod, String configMapName) {
        return pod.getSpec().getContainers().get(0).getEnvFrom().stream()
                .anyMatch(
                        source ->
                                source.getConfigMapRef() != null
                                        && configMapName.equals(
                                                source.getConfigMapRef().getName()));
    }

    @Test
    void secretAndConfigMapConfigurationReachesTheJob() throws Exception {
        String createdSecret = "seatunnel-application-e2e-secret";
        try (KubernetesApplicationClient application = deployApplication("configuration")) {
            String generatedConfigMap = "seatunnel-application-e2e-generated";
            String defaultConfigMap = application.getClusterId() + "-generated-default";
            try {
                awaitWorkersRunning(application, 1);
                V1Pod master = masterPod(application);
                assertEquals(
                        createdSecret,
                        environment(master, "E2E_JOB_MODE")
                                .getValueFrom()
                                .getSecretKeyRef()
                                .getName());
                assertEquals(
                        generatedConfigMap,
                        environment(master, "E2E_CONFIG_MAP_VALUE")
                                .getValueFrom()
                                .getConfigMapKeyRef()
                                .getName());
                assertEquals("literal-value", environment(master, "E2E_LITERAL").getValue());
                assertTrue(envFromConfigMap(master, RUNTIME_CONFIG_MAP));
                assertTrue(
                        mountedReadOnly(
                                master, "/opt/seatunnel/config/application-e2e.properties"));
                assertTrue(
                        mountedReadOnly(
                                master,
                                "/opt/seatunnel/config/application-e2e-default.properties"));
                assertTrue(mountedReadOnly(master, "/opt/seatunnel/custom/seatunnel.yaml"));
                assertTrue(mountedReadOnly(master, "/var/run/seatunnel/secrets"));
                assertTrue(mountedReadOnly(master, "/opt/seatunnel/config/jvm_options"));
                V1Service service =
                        core.readNamespacedService(application.getClusterId(), namespace, null);
                assertEquals("None", service.getSpec().getClusterIP());
                assertTrue(service.getSpec().getPublishNotReadyAddresses());
                assertEquals(
                        application.getClusterId(),
                        service.getSpec().getSelector().get(APPLICATION_LABEL));
                assertEquals("master", service.getSpec().getSelector().get(ROLE_LABEL));
                assertTrue(
                        master.getSpec()
                                .getContainers()
                                .get(0)
                                .getCommand()
                                .contains(
                                        "-Dseatunnel.config=/opt/seatunnel/custom/seatunnel.yaml"));
                assertTrue(
                        master.getSpec()
                                .getContainers()
                                .get(0)
                                .getCommand()
                                .contains(
                                        "-Dhazelcast.config=/opt/seatunnel/config/hazelcast-master.yaml"));
                assertTrue(
                        master.getSpec()
                                .getContainers()
                                .get(0)
                                .getCommand()
                                .contains("@/opt/seatunnel/config/jvm_master_options"));
                assertTrue(
                        master.getSpec()
                                .getContainers()
                                .get(0)
                                .getCommand()
                                .contains("@/opt/seatunnel/config/jvm_options"));
                assertTrue(
                        master.getSpec()
                                .getContainers()
                                .get(0)
                                .getCommand()
                                .contains("--add-opens=java.base/java.lang=ALL-UNNAMED"));
                for (V1Pod worker : workers(application)) {
                    assertTrue(
                            mountedReadOnly(
                                    worker, "/opt/seatunnel/config/application-e2e.properties"));
                    assertTrue(
                            mountedReadOnly(
                                    worker,
                                    "/opt/seatunnel/config/application-e2e-default.properties"));
                    assertTrue(mountedReadOnly(worker, "/opt/seatunnel/custom/seatunnel.yaml"));
                    assertTrue(mountedReadOnly(worker, "/var/run/seatunnel/secrets"));
                    assertEquals(
                            createdSecret,
                            environment(worker, "E2E_JOB_MODE")
                                    .getValueFrom()
                                    .getSecretKeyRef()
                                    .getName());
                    assertTrue(
                            worker.getSpec()
                                    .getContainers()
                                    .get(0)
                                    .getCommand()
                                    .contains(
                                            "-Dhazelcast.config=/opt/seatunnel/config/hazelcast-worker.yaml"));
                    assertTrue(
                            worker.getSpec()
                                    .getContainers()
                                    .get(0)
                                    .getCommand()
                                    .contains("@/opt/seatunnel/config/jvm_worker_options"));
                }
                V1Secret secret = core.readNamespacedSecret(createdSecret, namespace, null);
                assertFalse(secret.getMetadata().getOwnerReferences().isEmpty());
                assertTrue(secret.getData().containsKey("E2E_JOB_MODE"));
                V1ConfigMap generated =
                        core.readNamespacedConfigMap(generatedConfigMap, namespace, null);
                assertFalse(generated.getMetadata().getOwnerReferences().isEmpty());
                assertEquals(
                        "source=kubernetes-configmap",
                        generated.getData().get("application-e2e.properties"));
                V1ConfigMap generatedDefault =
                        core.readNamespacedConfigMap(defaultConfigMap, namespace, null);
                assertFalse(generatedDefault.getMetadata().getOwnerReferences().isEmpty());
                assertEquals(
                        "source=default-name",
                        generatedDefault.getData().get("application-e2e-default.properties"));
                // Substitution is resolved from the Pod environment; the batch job only succeeds
                // when job.mode, parallelism and the envFrom value are resolved on the master.
                awaitStatus(application, ApplicationStatus.SUCCEEDED);
            } finally {
                cancelAndAwait(application);
            }
            Awaitility.await()
                    .atMost(CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertTrue(
                                        missing(
                                                () ->
                                                        core.readNamespacedSecret(
                                                                createdSecret, namespace, null)));
                                assertTrue(
                                        missing(
                                                () ->
                                                        core.readNamespacedConfigMap(
                                                                generatedConfigMap,
                                                                namespace,
                                                                null)));
                                assertTrue(
                                        missing(
                                                () ->
                                                        core.readNamespacedConfigMap(
                                                                defaultConfigMap,
                                                                namespace,
                                                                null)));
                            });
            // The referenced ConfigMap is user-owned and must survive cancellation.
            assertNotNull(core.readNamespacedConfigMap(RUNTIME_CONFIG_MAP, namespace, null));
        }
    }

    @Test
    void secretJobConfigurationReplacesTheSubmittedOne() throws Exception {
        String createdSecret = "seatunnel-application-e2e-job-conf";
        try (KubernetesApplicationClient application =
                deployApplication("configuration-job-conf-secret")) {
            try {
                awaitWorkersRunning(application, 1);
                V1Pod master = masterPod(application);
                assertTrue(mountedReadOnly(master, "/etc/seatunnel-secrets/application"));
                V1Secret secret = core.readNamespacedSecret(createdSecret, namespace, null);
                assertFalse(secret.getMetadata().getOwnerReferences().isEmpty());
                assertTrue(secret.getData().containsKey("job.conf"));
                // The submitted job.conf refers to a sink that does not exist, so the batch job
                // can only succeed when the mounted job.conf replaced it before parsing.
                awaitStatus(application, ApplicationStatus.SUCCEEDED);
            } finally {
                cancelAndAwait(application);
            }
            Awaitility.await()
                    .atMost(CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertTrue(
                                            missing(
                                                    () ->
                                                            core.readNamespacedSecret(
                                                                    createdSecret,
                                                                    namespace,
                                                                    null))));
        }
    }

    @Test
    void configMapJobConfigurationReplacesTheSubmittedOne() throws Exception {
        String createdConfigMap = "seatunnel-application-e2e-job-conf";
        try (KubernetesApplicationClient application =
                deployApplication("configuration-job-conf-configmap")) {
            try {
                awaitWorkersRunning(application, 1);
                V1Pod master = masterPod(application);
                assertTrue(mountedReadOnly(master, "/opt/seatunnel/config/job.conf"));
                V1ConfigMap configMap =
                        core.readNamespacedConfigMap(createdConfigMap, namespace, null);
                assertFalse(configMap.getMetadata().getOwnerReferences().isEmpty());
                assertTrue(configMap.getData().containsKey("job.conf"));
                // The submitted job.conf refers to a sink that does not exist, so the batch job
                // can only succeed when the mounted ConfigMap job.conf replaced it.
                awaitStatus(application, ApplicationStatus.SUCCEEDED);
            } finally {
                cancelAndAwait(application);
            }
            Awaitility.await()
                    .atMost(CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertTrue(
                                            missing(
                                                    () ->
                                                            core.readNamespacedConfigMap(
                                                                    createdConfigMap,
                                                                    namespace,
                                                                    null))));
        }
    }

    @Test
    void referencedSecretAndConfigMapAreMountedAndPreserved() throws Exception {
        try (KubernetesApplicationClient application = deployApplication("configuration-ref")) {
            try {
                awaitWorkersRunning(application, 1);
                V1Pod master = masterPod(application);
                assertEquals(
                        REFERENCE_SECRET,
                        environment(master, "E2E_PARALLELISM")
                                .getValueFrom()
                                .getSecretKeyRef()
                                .getName());
                assertTrue(envFromSecret(master, REFERENCE_SECRET));
                assertTrue(mountedReadOnly(master, "/etc/seatunnel-secrets/application"));
                assertTrue(mountedReadOnly(master, "/opt/seatunnel/config/seatunnel.yaml"));
                // Workers are created by the master in-cluster and reuse the same pod
                // decorator chain, so the envFrom reference must reach them too.
                for (V1Pod worker : workers(application)) {
                    assertTrue(envFromSecret(worker, REFERENCE_SECRET));
                }
                awaitStatus(application, ApplicationStatus.SUCCEEDED);
            } finally {
                cancelAndAwait(application);
            }
            assertNotNull(core.readNamespacedSecret(REFERENCE_SECRET, namespace, null));
            assertNotNull(core.readNamespacedConfigMap(RUNTIME_CONFIG_MAP, namespace, null));
        }
    }
}
