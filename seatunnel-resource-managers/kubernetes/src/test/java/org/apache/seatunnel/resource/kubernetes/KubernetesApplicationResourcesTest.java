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

package org.apache.seatunnel.resource.kubernetes;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceEventHandler;
import org.apache.seatunnel.resource.kubernetes.cli.SeatunnelKubernetesApplicationCli;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesSecret;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.seatunnel.resource.kubernetes.worker.SeatunnelKubernetesApplicationWorker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1EnvFromSource;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1Toleration;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KubernetesApplicationResourcesTest {

    @TempDir Path temporary;

    private static final String mainClass = SeatunnelKubernetesApplicationCli.class.getName();

    @ParameterizedTest
    @ValueSource(strings = {"Always", "IfNotPresent", "Never"})
    void preservesKubernetesImagePullPolicies(String policy) {
        Map<String, String> options = options();
        options.put(KubernetesOptions.IMAGE_PULL_POLICY.key(), policy);
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        assertEquals(policy, parameters.getImagePullPolicy());
        assertEquals(
                policy,
                applicationJob(
                                KubernetesApplicationClusterDescriptor.assembleApplication(
                                        "app", mainClass, parameters))
                        .getInternalResource()
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getImagePullPolicy());
    }

    @Test
    void rejectsInvalidImagePullPolicy() {
        Map<String, String> options = options();
        options.put(KubernetesOptions.IMAGE_PULL_POLICY.key(), "always");
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                KubernetesApplicationConfig.from(
                                        specification(),
                                        ReadonlyConfig.fromMap(new HashMap<>(options))));
        assertTrue(failure.getMessage().contains("kubernetes.image-pull-policy"));
    }

    @Test
    void mountsExistingCheckpointClaimOnlyOnMaster() throws Exception {
        Map<String, String> options = options();
        options.put(KubernetesOptions.CHECKPOINT_PVC.key(), "existing-checkpoints");
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env {}", options);
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        KubernetesJob job =
                applicationJob(
                        KubernetesApplicationClusterDescriptor.assembleApplication(
                                "application", mainClass, parameters));
        V1Job jobResource = job.getInternalResource();
        assertEquals(
                "existing-checkpoints",
                volume(jobResource, KubernetesConstants.CHECKPOINT_VOLUME)
                        .getPersistentVolumeClaim()
                        .getClaimName());
        assertEquals(
                "/opt/seatunnel/checkpoints",
                mount(jobResource, KubernetesConstants.CHECKPOINT_VOLUME).getMountPath());
        jobResource.getMetadata().setUid("owner-uid");
        V1Pod worker =
                workerPod(
                                job,
                                "worker",
                                parameters,
                                specification.getWorkerSpecification(),
                                "cluster",
                                "master:5801")
                        .getInternalResource();
        assertTrue(
                worker.getSpec().getVolumes() == null || worker.getSpec().getVolumes().isEmpty());
        assertTrue(
                worker.getSpec().getContainers().get(0).getVolumeMounts() == null
                        || worker.getSpec().getContainers().get(0).getVolumeMounts().isEmpty());
    }

    @Test
    void buildsOwnedIsolatedResourcesWithoutShellInterpolation() throws Exception {
        Map<String, String> options = options();
        options.put(KubernetesOptions.IMAGE_PULL_SECRETS.key(), "registry-one,registry-two");
        options.put(KubernetesOptions.MASTER_LABELS.key(), "workload:control");
        options.put(KubernetesOptions.WORKER_LABELS.key(), "workload:data");
        options.put(KubernetesOptions.MASTER_ANNOTATIONS.key(), "owner:platform");
        options.put(KubernetesOptions.WORKER_ANNOTATIONS.key(), "owner:runtime");
        options.put(KubernetesOptions.MASTER_NODE_SELECTOR.key(), "pool:master");
        options.put(KubernetesOptions.WORKER_NODE_SELECTOR.key(), "pool:worker");
        options.put(KubernetesOptions.MASTER_TOLERATIONS.key(), "dedicated=seatunnel:NoSchedule");
        options.put(KubernetesOptions.WORKER_TOLERATIONS.key(), "batch:NoExecute;spot=true");
        options.put("kubernetes.configmap.runtime.ref", "seatunnel-runtime");
        options.put(
                "kubernetes.configmap.runtime.mount.seatunnel.yaml",
                "/opt/seatunnel/config/seatunnel.yaml");
        options.put(KubernetesOptions.NAMESPACE.key(), "analytics");
        options.put("submitter.private-setting", "must-not-be-localized");
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env {}", options);
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        List<KubernetesResource<?>> application =
                KubernetesApplicationClusterDescriptor.assembleApplication(
                        "app", mainClass, parameters);
        KubernetesJob job = applicationJob(application);
        V1Job jobResource = job.getInternalResource();
        jobResource.getMetadata().setUid("uid-1");
        assertTrue(
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand()
                        .contains(SeatunnelKubernetesApplicationCli.class.getName()));
        assertTrue(jobResource.getSpec().getSuspend());
        assertEquals(0, jobResource.getSpec().getBackoffLimit());
        assertEquals("Never", jobResource.getSpec().getTemplate().getSpec().getRestartPolicy());
        assertEquals(
                "control",
                jobResource.getSpec().getTemplate().getMetadata().getLabels().get("workload"));
        assertEquals(
                "platform",
                jobResource.getSpec().getTemplate().getMetadata().getAnnotations().get("owner"));
        assertEquals(
                "master",
                jobResource.getSpec().getTemplate().getSpec().getNodeSelector().get("pool"));
        List<V1Toleration> masterTolerations =
                jobResource.getSpec().getTemplate().getSpec().getTolerations();
        assertEquals(1, masterTolerations.size());
        assertEquals("dedicated", masterTolerations.get(0).getKey());
        assertEquals("Equal", masterTolerations.get(0).getOperator());
        assertEquals("seatunnel", masterTolerations.get(0).getValue());
        assertEquals("NoSchedule", masterTolerations.get(0).getEffect());
        assertEquals(
                "registry-two",
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getImagePullSecrets()
                        .get(1)
                        .getName());
        String encodedSpecification = envValue(jobResource, KubernetesConstants.SPECIFICATION_ENV);
        assertFalse(encodedSpecification.contains("must-not-be-localized"));
        KubernetesApplicationConfig restored =
                KubernetesApplicationConfig.read(encodedSpecification);
        assertEquals(specification.getJobId(), restored.getSpecification().getJobId());
        assertEquals(specification.getJobConfig(), restored.getSpecification().getJobConfig());
        assertEquals(
                specification.getWorkerSpecification(),
                restored.getSpecification().getWorkerSpecification());
        assertEquals("analytics", restored.getNamespace());
        assertEquals(parameters.getImage(), restored.getImage());
        assertEquals(parameters.getImagePullPolicy(), restored.getImagePullPolicy());
        assertEquals(parameters.getImagePullSecrets(), restored.getImagePullSecrets());
        assertEquals(parameters.getMasterLabels(), restored.getMasterLabels());
        assertEquals(parameters.getWorkerLabels(), restored.getWorkerLabels());
        assertEquals(parameters.getMasterAnnotations(), restored.getMasterAnnotations());
        assertEquals(parameters.getWorkerAnnotations(), restored.getWorkerAnnotations());
        assertEquals(parameters.getMasterNodeSelector(), restored.getMasterNodeSelector());
        assertEquals(parameters.getWorkerNodeSelector(), restored.getWorkerNodeSelector());
        assertEquals(parameters.getMasterTolerations(), restored.getMasterTolerations());
        assertEquals(parameters.getWorkerTolerations(), restored.getWorkerTolerations());
        String runtimeVolume = KubernetesConstants.CONFIG_VOLUME + "-0";
        assertEquals(
                "seatunnel-runtime", volume(jobResource, runtimeVolume).getConfigMap().getName());
        // Mounting one key through subPath keeps the rest of the image's configuration directory.
        assertEquals(
                "/opt/seatunnel/config/seatunnel.yaml",
                mount(jobResource, runtimeVolume).getMountPath());
        assertEquals("seatunnel.yaml", mount(jobResource, runtimeVolume).getSubPath());
        assertTrue(mount(jobResource, runtimeVolume).getReadOnly());
        KubernetesService service =
                (KubernetesService)
                        accompanyingResources(application).stream()
                                .filter(KubernetesService.class::isInstance)
                                .findFirst()
                                .orElseThrow(() -> new AssertionError("Missing Service"));
        assertEquals("None", service.getInternalResource().getSpec().getClusterIP());
        KubernetesPod workerPod =
                workerPod(
                        job,
                        "app-worker-0",
                        parameters,
                        specification.getWorkerSpecification(),
                        "isolated-cluster",
                        "10.0.0.1:5801");
        V1Pod worker = workerPod.getInternalResource();
        assertFalse(worker.getSpec().getAutomountServiceAccountToken());
        assertEquals("uid-1", worker.getMetadata().getOwnerReferences().get(0).getUid());
        assertEquals("data", worker.getMetadata().getLabels().get("workload"));
        assertEquals("runtime", worker.getMetadata().getAnnotations().get("owner"));
        assertEquals("worker", worker.getSpec().getNodeSelector().get("pool"));
        List<V1Toleration> workerTolerations = worker.getSpec().getTolerations();
        assertEquals(2, workerTolerations.size());
        assertEquals("batch", workerTolerations.get(0).getKey());
        assertEquals("Exists", workerTolerations.get(0).getOperator());
        assertNull(workerTolerations.get(0).getValue());
        assertEquals("NoExecute", workerTolerations.get(0).getEffect());
        assertEquals("spot", workerTolerations.get(1).getKey());
        assertEquals("Equal", workerTolerations.get(1).getOperator());
        assertEquals("true", workerTolerations.get(1).getValue());
        assertNull(workerTolerations.get(1).getEffect());
        assertEquals("registry-one", worker.getSpec().getImagePullSecrets().get(0).getName());
        assertEquals(
                "seatunnel-runtime", worker.getSpec().getVolumes().get(0).getConfigMap().getName());
        assertEquals(
                "/opt/seatunnel/config/seatunnel.yaml",
                worker.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath());
        assertEquals("java", worker.getSpec().getContainers().get(0).getCommand().get(0));
        List<String> workerCommand = worker.getSpec().getContainers().get(0).getCommand();
        int entrypoint =
                workerCommand.indexOf(SeatunnelKubernetesApplicationWorker.class.getName());
        assertTrue(entrypoint >= 0);
        assertEquals(
                Arrays.asList("isolated-cluster", "10.0.0.1:5801", "2"),
                workerCommand.subList(entrypoint + 1, workerCommand.size()));
        assertTrue(
                worker.getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand()
                        .contains(SeatunnelKubernetesApplicationWorker.class.getName()));
        assertTrue(
                worker.getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand()
                        .contains("-Dseatunnel.config=/opt/seatunnel/config/seatunnel.yaml"));
        assertTrue(
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand()
                        .contains("-Dseatunnel.config=/opt/seatunnel/config/seatunnel.yaml"));
        assertTrue(worker.getSpec().getContainers().get(0).getCommand().contains("10.0.0.1:5801"));
        assertTrue(
                worker.getSpec().getContainers().get(0).getCommand().contains("isolated-cluster"));
        assertEquals(
                "worker", worker.getMetadata().getLabels().get(KubernetesConstants.ROLE_LABEL));
        assertTrue(
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getMetadata()
                        .getLabels()
                        .entrySet()
                        .containsAll(
                                service.getInternalResource().getSpec().getSelector().entrySet()));

        assertEquals(
                5801,
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getReadinessProbe()
                        .getTcpSocket()
                        .getPort()
                        .getIntValue());
        assertTrue(
                jobResource
                                .getSpec()
                                .getTemplate()
                                .getSpec()
                                .getContainers()
                                .get(0)
                                .getStartupProbe()
                        != null);
        // PID 1 is the JVM itself, so a process probe cannot fail before the container exits.
        assertNull(worker.getSpec().getContainers().get(0).getLivenessProbe());
    }

    private static ApplicationSpecification specification() {
        return SeatunnelApplicationConfig.parse("env { job.mode = BATCH }", options());
    }

    @Test
    void mountsReferencedSecretWithoutCreatingResources() throws Exception {
        Map<String, String> options = options();
        options.put(KubernetesOptions.SECRET_REF.key(), "platform-secret");
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        List<KubernetesResource<?>> application =
                KubernetesApplicationClusterDescriptor.assembleApplication(
                        "app", mainClass, parameters);
        assertTrue(
                accompanyingResources(application).stream()
                        .noneMatch(KubernetesSecret.class::isInstance));
        V1Job job = applicationJob(application).getInternalResource();
        assertEquals(
                "platform-secret",
                volume(job, KubernetesConstants.APPLICATION_VOLUME).getSecret().getSecretName());
        assertEquals(
                "/etc/seatunnel-secrets/application",
                mount(job, KubernetesConstants.APPLICATION_VOLUME).getMountPath());
        assertTrue(mount(job, KubernetesConstants.APPLICATION_VOLUME).getReadOnly());
    }

    @Test
    void createsConfiguredSecretDataAndMountsIt() throws Exception {
        Map<String, String> options = options();
        options.put(KubernetesOptions.SECRET_NAME.key(), "job-secrets");
        options.put("kubernetes.secret.data.MYSQL_PASSWORD", "top-secret");
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        List<KubernetesResource<?>> application =
                KubernetesApplicationClusterDescriptor.assembleApplication(
                        "app", mainClass, parameters);
        assertEquals(2, accompanyingResources(application).size());
        KubernetesResource<?> configurationSecret =
                accompanyingResources(application).stream()
                        .filter(KubernetesSecret.class::isInstance)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Missing created Secret"));
        V1Secret secret = ((KubernetesSecret) configurationSecret).getInternalResource();
        assertEquals("job-secrets", secret.getMetadata().getName());
        assertEquals(
                "app", secret.getMetadata().getLabels().get(KubernetesConstants.APPLICATION_LABEL));
        assertEquals("Opaque", secret.getType());
        assertEquals("top-secret", secret.getStringData().get("MYSQL_PASSWORD"));
        assertNull(secret.getMetadata().getOwnerReferences());
        V1Job job = applicationJob(application).getInternalResource();
        assertEquals(
                "job-secrets",
                volume(job, KubernetesConstants.APPLICATION_VOLUME).getSecret().getSecretName());
    }

    @Test
    void rejectsConflictingApplicationSecretOptions() {
        Map<String, String> conflicting = options();
        conflicting.put(KubernetesOptions.SECRET_REF.key(), "platform-secret");
        conflicting.put("kubernetes.secret.data.PASSWORD", "value");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(conflicting))));
        Map<String, String> nameOnly = options();
        nameOnly.put(KubernetesOptions.SECRET_NAME.key(), "job-secrets");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(nameOnly))));
    }

    @Test
    void injectsDeclaredEnvironmentVariablesIntoMasterAndWorkerPods() throws Exception {
        Map<String, String> options = options();
        options.put("kubernetes.env.LITERAL", "value");
        options.put("kubernetes.env.FROM_SECRET", "secret:db-secret/password");
        options.put("kubernetes.env.FROM_CONFIG", "configmap:runtime-cm/path");
        options.put("kubernetes.env-from.secret", "extra-secret");
        options.put("kubernetes.env-from.configmap", "extra-config");
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        List<KubernetesResource<?>> application =
                KubernetesApplicationClusterDescriptor.assembleApplication(
                        "app", mainClass, parameters);
        KubernetesJob applicationJob = applicationJob(application);
        V1Job job = applicationJob.getInternalResource();
        List<V1EnvVar> masterEnvironment =
                job.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertEquals("value", env(masterEnvironment, "LITERAL").getValue());
        assertEquals(
                "db-secret",
                env(masterEnvironment, "FROM_SECRET").getValueFrom().getSecretKeyRef().getName());
        assertEquals(
                "password",
                env(masterEnvironment, "FROM_SECRET").getValueFrom().getSecretKeyRef().getKey());
        assertEquals(
                "runtime-cm",
                env(masterEnvironment, "FROM_CONFIG")
                        .getValueFrom()
                        .getConfigMapKeyRef()
                        .getName());
        assertEquals(
                "path",
                env(masterEnvironment, "FROM_CONFIG").getValueFrom().getConfigMapKeyRef().getKey());
        List<V1EnvFromSource> masterEnvFrom =
                job.getSpec().getTemplate().getSpec().getContainers().get(0).getEnvFrom();
        assertEquals("extra-secret", masterEnvFrom.get(0).getSecretRef().getName());
        assertEquals("extra-config", masterEnvFrom.get(1).getConfigMapRef().getName());
        V1Pod worker =
                workerPod(
                                applicationJob,
                                "app-worker-0",
                                parameters,
                                specification().getWorkerSpecification(),
                                "cluster",
                                "master:5801")
                        .getInternalResource();
        List<V1EnvVar> workerEnvironment = worker.getSpec().getContainers().get(0).getEnv();
        assertEquals(4, workerEnvironment.size());
        assertEquals(
                "db-secret",
                env(workerEnvironment, "FROM_SECRET").getValueFrom().getSecretKeyRef().getName());
        assertEquals(2, worker.getSpec().getContainers().get(0).getEnvFrom().size());
        assertEquals(
                "extra-secret",
                worker.getSpec()
                        .getContainers()
                        .get(0)
                        .getEnvFrom()
                        .get(0)
                        .getSecretRef()
                        .getName());
    }

    @Test
    void keepsCredentialValuesOutOfTheLocalizedSpecification() throws Exception {
        Map<String, String> options = options();
        options.put(KubernetesOptions.SECRET_NAME.key(), "job-secrets");
        options.put("kubernetes.secret.data.PASSWORD", "super-secret-password");
        options.put(
                "kubernetes.configmap.runtime.data.seatunnel.yaml",
                "engine:\n  checkpoint:\n    interval: 10000\n");
        options.put(
                "kubernetes.configmap.runtime.mount.seatunnel.yaml",
                "/opt/seatunnel/config/seatunnel.yaml");
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        List<KubernetesResource<?>> application =
                KubernetesApplicationClusterDescriptor.assembleApplication(
                        "app", mainClass, parameters);
        String localized =
                new String(
                        Base64.getDecoder()
                                .decode(
                                        envValue(
                                                applicationJob(application).getInternalResource(),
                                                KubernetesConstants.SPECIFICATION_ENV)),
                        StandardCharsets.UTF_8);
        assertTrue(localized.contains("kubernetes.secret.data.PASSWORD="));
        assertTrue(localized.contains("kubernetes.configmap.runtime.data.seatunnel.yaml="));
        assertFalse(localized.contains("super-secret-password"));
        assertFalse(localized.contains("interval: 10000"));
    }

    @Test
    void rejectsLocalizedSpecificationsAboveTheEnvTransportLimit() {
        Map<String, String> options = options();
        String oversizedJobOption = new String(new char[80 * 1024]).replace('\0', 'x');
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse(
                        "env { job.mode = BATCH }\nenv { oversized = \""
                                + oversizedJobOption
                                + "\" }",
                        options);
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        IllegalStateException failure =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                KubernetesApplicationClusterDescriptor.assembleApplication(
                                        "app", mainClass, parameters));
        assertTrue(failure.getMessage().contains("too large"), failure.getMessage());
    }

    @Test
    void keepsTheLocalizedSpecificationOffWorkerPods() throws Exception {
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options())));
        KubernetesJob applicationJob =
                applicationJob(
                        KubernetesApplicationClusterDescriptor.assembleApplication(
                                "app", mainClass, parameters));
        V1Pod worker =
                workerPod(
                                applicationJob,
                                "app-worker-0",
                                parameters,
                                specification().getWorkerSpecification(),
                                "cluster",
                                "master:5801")
                        .getInternalResource();
        assertTrue(
                worker.getSpec().getContainers().get(0).getEnv().stream()
                        .noneMatch(
                                variable ->
                                        KubernetesConstants.SPECIFICATION_ENV.equals(
                                                variable.getName())));
        assertTrue(
                applicationJob.getInternalResource().getSpec().getTemplate().getSpec()
                        .getContainers().get(0).getEnv().stream()
                        .anyMatch(
                                variable ->
                                        KubernetesConstants.SPECIFICATION_ENV.equals(
                                                variable.getName())));
    }

    @Test
    void rejectsInvalidTolerations() {
        Map<String, String> unknownEffect = options();
        unknownEffect.put(KubernetesOptions.MASTER_TOLERATIONS.key(), "dedicated=seatunnel:Never");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(unknownEffect))));
        Map<String, String> missingKey = options();
        missingKey.put(KubernetesOptions.WORKER_TOLERATIONS.key(), "=seatunnel:NoSchedule");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(missingKey))));
        Map<String, String> missingValue = options();
        missingValue.put(KubernetesOptions.WORKER_TOLERATIONS.key(), "dedicated=:NoSchedule");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(missingValue))));
        Map<String, String> emptyEntry = options();
        emptyEntry.put(KubernetesOptions.WORKER_TOLERATIONS.key(), "batch=true;;spot=true");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(emptyEntry))));
    }

    @Test
    void rejectsConfigMapMountsThatCollideWithReservedPaths() {
        Map<String, String> insideSecretDirectory = options();
        insideSecretDirectory.put("kubernetes.configmap.runtime.data.job.conf", "content");
        insideSecretDirectory.put(
                "kubernetes.configmap.runtime.mount.job.conf",
                KubernetesConstants.APPLICATION_SECRET_DIRECTORY + "/job.conf");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(insideSecretDirectory))));
        Map<String, String> aboveSecretDirectory = options();
        aboveSecretDirectory.put("kubernetes.configmap.runtime.data.other", "content");
        aboveSecretDirectory.put(
                "kubernetes.configmap.runtime.mount.other", "/etc/seatunnel-secrets");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(aboveSecretDirectory))));
        Map<String, String> checkpointDirectory = options();
        checkpointDirectory.put("kubernetes.configmap.runtime.data.other", "content");
        checkpointDirectory.put(
                "kubernetes.configmap.runtime.mount.other",
                KubernetesOptions.CHECKPOINT_DIRECTORY.defaultValue());
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(checkpointDirectory))));
        Map<String, String> samePathTwice = options();
        samePathTwice.put("kubernetes.configmap.first.data.a.properties", "one");
        samePathTwice.put(
                "kubernetes.configmap.first.mount.a.properties",
                "/opt/seatunnel/config/application.properties");
        samePathTwice.put("kubernetes.configmap.second.data.b.properties", "two");
        samePathTwice.put(
                "kubernetes.configmap.second.mount.b.properties",
                "/opt/seatunnel/config/application.properties");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(samePathTwice))));
    }

    @Test
    void rejectsInvalidEnvironmentVariableEntries() {
        Map<String, String> reserved = options();
        reserved.put("kubernetes.env.SEATUNNEL_APPLICATION_SPECIFICATION", "value");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(reserved))));
        Map<String, String> malformed = options();
        malformed.put("kubernetes.env.BAD_NAME!", "value");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(malformed))));
        Map<String, String> badReference = options();
        badReference.put("kubernetes.env.FROM_SECRET", "secret:missing-key");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(badReference))));
        Map<String, String> badEnvFrom = options();
        badEnvFrom.put("kubernetes.env-from.other", "value");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(badEnvFrom))));
        Map<String, String> emptyEnvFrom = options();
        emptyEnvFrom.put("kubernetes.env-from.secret", "");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(emptyEnvFrom))));
    }

    @Test
    void createsConfiguredConfigMapsAndMountsThemThroughSubPaths() throws Exception {
        Map<String, String> options = options();
        options.put("kubernetes.configmap.runtime.data.seatunnel.yaml", "engine:\n  job: {}\n");
        options.put(
                "kubernetes.configmap.runtime.data.log4j2_client.properties",
                "rootLogger.level=INFO");
        options.put(
                "kubernetes.configmap.runtime.mount.seatunnel.yaml",
                "/opt/seatunnel/config/seatunnel.yaml");
        options.put(
                "kubernetes.configmap.runtime.mount.log4j2_client.properties",
                "/opt/seatunnel/config/log4j2_client.properties");
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        List<KubernetesResource<?>> application =
                KubernetesApplicationClusterDescriptor.assembleApplication(
                        "app", mainClass, parameters);
        KubernetesResource<?> configuration =
                accompanyingResources(application).stream()
                        .filter(KubernetesConfigMap.class::isInstance)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Missing created ConfigMap"));
        V1ConfigMap configMap = ((KubernetesConfigMap) configuration).getInternalResource();
        assertEquals("app-runtime", configMap.getMetadata().getName());
        assertEquals("engine:\n  job: {}\n", configMap.getData().get("seatunnel.yaml"));
        V1Job job = applicationJob(application).getInternalResource();
        String volumeName = KubernetesConstants.CONFIG_VOLUME + "-0";
        assertEquals("app-runtime", volume(job, volumeName).getConfigMap().getName());
        assertEquals(
                "/opt/seatunnel/config/seatunnel.yaml",
                subPath(job, volumeName, "seatunnel.yaml").getMountPath());
        assertEquals(
                "/opt/seatunnel/config/log4j2_client.properties",
                subPath(job, volumeName, "log4j2_client.properties").getMountPath());
        assertTrue(subPath(job, volumeName, "seatunnel.yaml").getReadOnly());
    }

    @Test
    void mountsReferencedConfigMapKeysWithOverrides() throws Exception {
        Map<String, String> options = options();
        options.put("kubernetes.configmap.shared.ref", "platform-runtime");
        options.put(
                "kubernetes.configmap.shared.mount.seatunnel.yaml",
                "/opt/seatunnel/config/seatunnel.yaml");
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        List<KubernetesResource<?>> application =
                KubernetesApplicationClusterDescriptor.assembleApplication(
                        "app", mainClass, parameters);
        assertTrue(
                accompanyingResources(application).stream()
                        .noneMatch(KubernetesConfigMap.class::isInstance));
        V1Job job = applicationJob(application).getInternalResource();
        String volumeName = KubernetesConstants.CONFIG_VOLUME + "-0";
        assertEquals("platform-runtime", volume(job, volumeName).getConfigMap().getName());
        assertEquals(
                "/opt/seatunnel/config/seatunnel.yaml",
                subPath(job, volumeName, "seatunnel.yaml").getMountPath());
    }

    @Test
    void rejectsInvalidConfigMapEntries() {
        Map<String, String> conflicting = options();
        conflicting.put("kubernetes.configmap.shared.ref", "platform-runtime");
        conflicting.put("kubernetes.configmap.shared.data.seatunnel.yaml", "content");
        conflicting.put(
                "kubernetes.configmap.shared.mount.seatunnel.yaml",
                "/opt/seatunnel/config/seatunnel.yaml");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(conflicting))));
        Map<String, String> referenceWithoutMount = options();
        referenceWithoutMount.put("kubernetes.configmap.shared.ref", "platform-runtime");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(referenceWithoutMount))));
        Map<String, String> unknownMount = options();
        unknownMount.put("kubernetes.configmap.runtime.data.seatunnel.yaml", "content");
        unknownMount.put(
                "kubernetes.configmap.runtime.mount.other.yaml",
                "/opt/seatunnel/config/other.yaml");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(unknownMount))));
        Map<String, String> missingMount = options();
        missingMount.put("kubernetes.configmap.runtime.data.seatunnel.yaml", "content");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(missingMount))));
    }

    @Test
    void createsConfigMapsWithAnExplicitName() throws Exception {
        Map<String, String> options = options();
        options.put("kubernetes.configmap.generated.name", "seatunnel-generated-config");
        options.put("kubernetes.configmap.generated.data.application.properties", "mode=batch");
        options.put(
                "kubernetes.configmap.generated.mount.application.properties",
                "/opt/seatunnel/config/application.properties");
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        List<KubernetesResource<?>> application =
                KubernetesApplicationClusterDescriptor.assembleApplication(
                        "app", mainClass, parameters);
        KubernetesResource<?> configuration =
                accompanyingResources(application).stream()
                        .filter(KubernetesConfigMap.class::isInstance)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Missing created ConfigMap"));
        assertEquals(
                "seatunnel-generated-config",
                ((KubernetesConfigMap) configuration)
                        .getInternalResource()
                        .getMetadata()
                        .getName());
        assertEquals(
                "app",
                ((KubernetesConfigMap) configuration)
                        .getInternalResource()
                        .getMetadata()
                        .getLabels()
                        .get(KubernetesConstants.APPLICATION_LABEL));
        assertEquals(
                "seatunnel-generated-config",
                volume(
                                applicationJob(application).getInternalResource(),
                                KubernetesConstants.CONFIG_VOLUME + "-0")
                        .getConfigMap()
                        .getName());
    }

    @Test
    void rejectsInvalidCreatedConfigMapNames() {
        Map<String, String> namedReference = options();
        namedReference.put("kubernetes.configmap.shared.name", "unused-name");
        namedReference.put("kubernetes.configmap.shared.ref", "platform-runtime");
        namedReference.put(
                "kubernetes.configmap.shared.mount.seatunnel.yaml",
                "/opt/seatunnel/config/seatunnel.yaml");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(namedReference))));
        Map<String, String> invalidName = options();
        invalidName.put("kubernetes.configmap.generated.name", "Invalid_Name");
        invalidName.put("kubernetes.configmap.generated.data.application.properties", "mode=batch");
        invalidName.put(
                "kubernetes.configmap.generated.mount.application.properties",
                "/opt/seatunnel/config/application.properties");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(invalidName))));
        Map<String, String> nameWithoutData = options();
        nameWithoutData.put("kubernetes.configmap.generated.name", "seatunnel-generated-config");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(nameWithoutData))));
    }

    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "Job-config paths are validated as container paths, which a Windows drive letter cannot express")
    void keepsTheSubmittedJobConfigurationWithoutAConfiguredJobConfigFile() throws Exception {
        Path seatunnelHome = temporary.resolve("seatunnel-home");
        Path jobConfigFile = seatunnelHome.resolve("config").resolve("job.conf");
        Files.createDirectories(jobConfigFile.getParent());
        Files.write(jobConfigFile, "env { job.mode = STREAMING }".getBytes(StandardCharsets.UTF_8));
        Map<String, String> options = options();
        options.put(KubernetesOptions.SEATUNNEL_HOME.key(), seatunnelHome.toString());
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env { job.mode = BATCH }", options);
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        StringWriter writer = new StringWriter();
        parameters.write(writer);
        // The option defaults to null: without an explicit kubernetes.job-config-file the mounted
        // file is ignored and nothing is localized.
        assertFalse(writer.toString().contains(KubernetesOptions.JOB_CONFIG_FILE.key()));
        String encoded =
                Base64.getEncoder()
                        .encodeToString(writer.toString().getBytes(StandardCharsets.UTF_8));
        KubernetesApplicationConfig restored = KubernetesApplicationConfig.read(encoded);
        assertEquals("env { job.mode = BATCH }", restored.getSpecification().getJobConfig());
    }

    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "Job-config paths are validated as container paths, which a Windows drive letter cannot express")
    void replacesInlineJobConfigurationWithAMountedSecretFile() throws Exception {
        Path secretDirectory = temporary.resolve("secrets");
        Path jobConfigFile = secretDirectory.resolve("job.conf");
        Files.createDirectories(secretDirectory);
        Files.write(jobConfigFile, "env { job.mode = STREAMING }".getBytes(StandardCharsets.UTF_8));
        Map<String, String> options = options();
        options.put(KubernetesOptions.SECRET_DIRECTORY.key(), secretDirectory.toString());
        // A Secret-provided job file is selected explicitly.
        options.put(KubernetesOptions.JOB_CONFIG_FILE.key(), jobConfigFile.toString());
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env { job.mode = BATCH }", options);
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        StringWriter writer = new StringWriter();
        parameters.write(writer);
        String encoded =
                Base64.getEncoder()
                        .encodeToString(writer.toString().getBytes(StandardCharsets.UTF_8));
        KubernetesApplicationConfig restored = KubernetesApplicationConfig.read(encoded);
        assertEquals("env { job.mode = STREAMING }", restored.getSpecification().getJobConfig());
    }

    @Test
    void rejectsSubmissionsWithoutAnyJobConfiguration() {
        Map<String, String> options = options();
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                SeatunnelApplicationConfig.parse("", options),
                                ReadonlyConfig.fromMap(new HashMap<>(options))));
    }

    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "Job-config paths are validated as container paths, which a Windows drive letter cannot express")
    void acceptsSubmissionsThatRelyOnAMountedJobConfigFile() throws Exception {
        Path jobConfigFile = temporary.resolve("mounted-job.conf");
        Files.write(jobConfigFile, "env { job.mode = STREAMING }".getBytes(StandardCharsets.UTF_8));
        Map<String, String> options = options();
        options.put(KubernetesOptions.JOB_CONFIG_FILE.key(), jobConfigFile.toString());
        ApplicationSpecification specification = SeatunnelApplicationConfig.parse("", options);
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        assertEquals("", parameters.getSpecification().getJobConfig());
        assertEquals(jobConfigFile.toString(), parameters.getJobConfigFile());
        StringWriter writer = new StringWriter();
        parameters.write(writer);
        String encoded =
                Base64.getEncoder()
                        .encodeToString(writer.toString().getBytes(StandardCharsets.UTF_8));
        KubernetesApplicationConfig restored = KubernetesApplicationConfig.read(encoded);
        assertEquals("env { job.mode = STREAMING }", restored.getSpecification().getJobConfig());
    }

    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "Job-config paths are validated as container paths, which a Windows drive letter cannot express")
    void failsWhenTheMountedJobConfigFileIsMissingAndNothingWasSubmitted() throws Exception {
        Path jobConfigFile = temporary.resolve("missing-job.conf");
        Map<String, String> options = options();
        options.put(KubernetesOptions.JOB_CONFIG_FILE.key(), jobConfigFile.toString());
        ApplicationSpecification specification = SeatunnelApplicationConfig.parse("", options);
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        StringWriter writer = new StringWriter();
        parameters.write(writer);
        String encoded =
                Base64.getEncoder()
                        .encodeToString(writer.toString().getBytes(StandardCharsets.UTF_8));
        IOException failure =
                assertThrows(IOException.class, () -> KubernetesApplicationConfig.read(encoded));
        assertTrue(failure.getMessage().contains("carries no inline job configuration"));
    }

    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "Job-config paths are validated as container paths, which a Windows drive letter cannot express")
    void doesNotTransportTheInlineJobWhenAJobConfigFileIsConfigured() throws Exception {
        Path jobConfigFile = temporary.resolve("job.conf");
        Files.write(jobConfigFile, "env { job.mode = STREAMING }".getBytes(StandardCharsets.UTF_8));
        Map<String, String> options = options();
        options.put(KubernetesOptions.JOB_CONFIG_FILE.key(), jobConfigFile.toString());
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse(
                        "env { job.mode = BATCH, password = \"inline-secret\" }", options);
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        StringWriter writer = new StringWriter();
        parameters.write(writer);
        // The mounted file supplies the job, so the submitted text must not travel in the spec.
        assertFalse(writer.toString().contains(SeatunnelApplicationConfig.JOB_CONFIG_KEY));
        assertFalse(writer.toString().contains("inline-secret"));
        assertTrue(writer.toString().contains(KubernetesOptions.JOB_CONFIG_FILE.key()));
    }

    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "Job-config paths are validated as container paths, which a Windows drive letter cannot express")
    void resolvesJobConfigFileNamesUnderTheConfigDirectory() throws Exception {
        Path seatunnelHome = temporary.resolve("seatunnel-home");
        Path jobConfigFile = seatunnelHome.resolve("config").resolve("custom-job.conf");
        Files.createDirectories(jobConfigFile.getParent());
        Files.write(jobConfigFile, "env { job.mode = STREAMING }".getBytes(StandardCharsets.UTF_8));
        Map<String, String> options = options();
        options.put(KubernetesOptions.SEATUNNEL_HOME.key(), seatunnelHome.toString());
        options.put(KubernetesOptions.JOB_CONFIG_FILE.key(), "custom-job.conf");
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env { job.mode = BATCH }", options);
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        StringWriter writer = new StringWriter();
        parameters.write(writer);
        String encoded =
                Base64.getEncoder()
                        .encodeToString(writer.toString().getBytes(StandardCharsets.UTF_8));
        KubernetesApplicationConfig restored = KubernetesApplicationConfig.read(encoded);
        assertEquals("env { job.mode = STREAMING }", restored.getSpecification().getJobConfig());
    }

    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "Job-config paths are validated as container paths, which a Windows drive letter cannot express")
    void replacesInlineJobConfigurationWithAFileFromAConfigMap() throws Exception {
        Path jobConfigFile = temporary.resolve("config").resolve("job.conf");
        Files.createDirectories(jobConfigFile.getParent());
        Files.write(
                jobConfigFile,
                "env { job.mode = STREAMING, parallelism = ${E2E_PARALLELISM} }"
                        .getBytes(StandardCharsets.UTF_8));
        Map<String, String> options = options();
        options.put(KubernetesOptions.JOB_CONFIG_FILE.key(), jobConfigFile.toString());
        options.put("kubernetes.configmap.job-conf.name", "seatunnel-job-conf");
        options.put(
                "kubernetes.configmap.job-conf.data.job.conf",
                "env { job.mode = STREAMING, parallelism = ${E2E_PARALLELISM} }");
        options.put("kubernetes.configmap.job-conf.mount.job.conf", jobConfigFile.toString());
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env { job.mode = BATCH }", options);
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        // The master reads the mounted file instead of the inline configuration.
        StringWriter writer = new StringWriter();
        parameters.write(writer);
        String encoded =
                Base64.getEncoder()
                        .encodeToString(writer.toString().getBytes(StandardCharsets.UTF_8));
        KubernetesApplicationConfig restored = KubernetesApplicationConfig.read(encoded);
        // The file content replaces the inline job verbatim; the master resolves the placeholder
        // against the Pod environment when it parses the job.
        assertEquals(
                "env { job.mode = STREAMING, parallelism = ${E2E_PARALLELISM} }",
                restored.getSpecification().getJobConfig());
        // The created ConfigMap is mounted as one subPath file at that exact container path.
        List<KubernetesResource<?>> application =
                KubernetesApplicationClusterDescriptor.assembleApplication(
                        "app", mainClass, parameters);
        V1Job job = applicationJob(application).getInternalResource();
        V1Volume volume = configMapVolume(job, "seatunnel-job-conf");
        assertEquals(
                jobConfigFile.toString(),
                subPath(job, volume.getName(), "job.conf").getMountPath());
        assertEquals("job.conf", subPath(job, volume.getName(), "job.conf").getSubPath());
    }

    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "Job-config paths are validated as container paths, which a Windows drive letter cannot express")
    void keepsTheSubmittedJobConfigurationWhenTheOptionIsEmpty() throws Exception {
        Path seatunnelHome = temporary.resolve("seatunnel-home");
        Path jobConfigFile = seatunnelHome.resolve("config").resolve("job.conf");
        Files.createDirectories(jobConfigFile.getParent());
        Files.write(jobConfigFile, "env { job.mode = STREAMING }".getBytes(StandardCharsets.UTF_8));
        Map<String, String> options = options();
        options.put(KubernetesOptions.SEATUNNEL_HOME.key(), seatunnelHome.toString());
        options.put(KubernetesOptions.JOB_CONFIG_FILE.key(), "");
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env { job.mode = BATCH }", options);
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        StringWriter writer = new StringWriter();
        parameters.write(writer);
        String encoded =
                Base64.getEncoder()
                        .encodeToString(writer.toString().getBytes(StandardCharsets.UTF_8));
        KubernetesApplicationConfig restored = KubernetesApplicationConfig.read(encoded);
        assertEquals("env { job.mode = BATCH }", restored.getSpecification().getJobConfig());
    }

    @Test
    void rejectsRelativeJobConfigFiles() {
        Map<String, String> relative = options();
        relative.put(KubernetesOptions.JOB_CONFIG_FILE.key(), "config/job.conf");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(relative))));
    }

    @Test
    void usesConfiguredMountDirectories() throws Exception {
        Map<String, String> options = options();
        options.put(KubernetesOptions.SECRET_DIRECTORY.key(), "/var/run/seatunnel/secrets");
        options.put(KubernetesOptions.CHECKPOINT_DIRECTORY.key(), "/var/run/seatunnel/checkpoints");
        options.put(KubernetesOptions.CHECKPOINT_PVC.key(), "existing-checkpoints");
        options.put(KubernetesOptions.SECRET_REF.key(), "platform-secret");
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        List<KubernetesResource<?>> application =
                KubernetesApplicationClusterDescriptor.assembleApplication(
                        "app", mainClass, parameters);
        V1Job job = applicationJob(application).getInternalResource();
        assertEquals(
                "/var/run/seatunnel/secrets",
                mount(job, KubernetesConstants.APPLICATION_VOLUME).getMountPath());
        assertEquals(
                "/var/run/seatunnel/checkpoints",
                mount(job, KubernetesConstants.CHECKPOINT_VOLUME).getMountPath());
        StringWriter writer = new StringWriter();
        parameters.write(writer);
        String encoded =
                Base64.getEncoder()
                        .encodeToString(writer.toString().getBytes(StandardCharsets.UTF_8));
        KubernetesApplicationConfig restored = KubernetesApplicationConfig.read(encoded);
        assertEquals("/var/run/seatunnel/secrets", restored.getSecretDirectory());
        assertEquals("/var/run/seatunnel/checkpoints", restored.getCheckpointDirectory());
    }

    @Test
    void rejectsRelativeMountDirectories() {
        Map<String, String> relativeSecret = options();
        relativeSecret.put(KubernetesOptions.SECRET_DIRECTORY.key(), "secrets");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(relativeSecret))));
        Map<String, String> relativeCheckpoint = options();
        relativeCheckpoint.put(KubernetesOptions.CHECKPOINT_DIRECTORY.key(), "checkpoints");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(relativeCheckpoint))));
    }

    @Test
    void buildsClasspathFromTheConfiguredSeatunnelHome() {
        Map<String, String> options = options();
        options.put(KubernetesOptions.SEATUNNEL_HOME.key(), "/custom/seatunnel");
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        KubernetesJob job =
                applicationJob(
                        KubernetesApplicationClusterDescriptor.assembleApplication(
                                "app", mainClass, parameters));
        List<String> command =
                job.getInternalResource()
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand();
        // Only jars are on the classpath: the config directory stays off it so engine and logging
        // configuration come exclusively from the explicit -D flags.
        assertEquals(
                "/custom/seatunnel/starter/seatunnel-starter.jar:"
                        + "/custom/seatunnel/starter/logging/*:"
                        + "/custom/seatunnel/lib/*:"
                        + "/custom/seatunnel/resource-managers/kubernetes/*",
                command.get(command.indexOf("-cp") + 1));
    }

    @Test
    void addsRoleSpecificHazelcastConfiguration() throws Exception {
        Map<String, String> options = options();
        options.put(KubernetesOptions.MASTER_HAZELCAST_CONFIG.key(), "hazelcast-master.yaml");
        options.put(
                KubernetesOptions.WORKER_HAZELCAST_CONFIG.key(),
                "/etc/seatunnel/hazelcast-worker.yaml");
        ApplicationSpecification specification = specification();
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        KubernetesJob job =
                applicationJob(
                        KubernetesApplicationClusterDescriptor.assembleApplication(
                                "app", mainClass, parameters));
        assertTrue(
                job.getInternalResource()
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand()
                        .contains(
                                "-Dhazelcast.config=/opt/seatunnel/config/hazelcast-master.yaml"));
        V1Pod worker =
                workerPod(
                                job,
                                "app-worker-0",
                                parameters,
                                specification.getWorkerSpecification(),
                                "cluster",
                                "master:5801")
                        .getInternalResource();
        assertTrue(
                worker.getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand()
                        .contains("-Dhazelcast.config=/etc/seatunnel/hazelcast-worker.yaml"));
    }

    @Test
    void rejectsInvalidHazelcastConfigurationFiles() {
        Map<String, String> invalidSuffix = options();
        invalidSuffix.put(KubernetesOptions.MASTER_HAZELCAST_CONFIG.key(), "hazelcast.properties");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(invalidSuffix))));
        Map<String, String> traversal = options();
        traversal.put(KubernetesOptions.WORKER_HAZELCAST_CONFIG.key(), "../hazelcast-worker.yaml");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(traversal))));
    }

    @Test
    void appliesJvmOptionsFilesAndMandatoryModuleFlags() throws Exception {
        ApplicationSpecification specification = specification();
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options())));
        KubernetesJob job =
                applicationJob(
                        KubernetesApplicationClusterDescriptor.assembleApplication(
                                "app", mainClass, parameters));
        List<String> masterCommand =
                job.getInternalResource()
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand();
        assertTrue(masterCommand.contains("--add-opens=java.base/java.lang=ALL-UNNAMED"));
        assertTrue(masterCommand.contains("@/opt/seatunnel/config/jvm_options"));
        assertTrue(masterCommand.contains("@/opt/seatunnel/config/jvm_master_options"));
        V1Pod worker =
                workerPod(
                                job,
                                "app-worker-0",
                                parameters,
                                specification.getWorkerSpecification(),
                                "cluster",
                                "master:5801")
                        .getInternalResource();
        assertTrue(
                worker.getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand()
                        .contains("@/opt/seatunnel/config/jvm_worker_options"));
        assertTrue(
                worker.getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand()
                        .contains("@/opt/seatunnel/config/jvm_options"));

        Map<String, String> overrides = options();
        overrides.put(KubernetesOptions.MASTER_JVM_OPTIONS.key(), "/etc/seatunnel/custom.options");
        overrides.put(KubernetesOptions.WORKER_JVM_OPTIONS.key(), "disabled");
        KubernetesApplicationConfig overridden =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(overrides)));
        KubernetesJob overriddenJob =
                applicationJob(
                        KubernetesApplicationClusterDescriptor.assembleApplication(
                                "app", mainClass, overridden));
        assertTrue(
                overriddenJob
                        .getInternalResource()
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand()
                        .contains("@/etc/seatunnel/custom.options"));
        V1Pod overriddenWorker =
                workerPod(
                                overriddenJob,
                                "app-worker-0",
                                overridden,
                                specification.getWorkerSpecification(),
                                "cluster",
                                "master:5801")
                        .getInternalResource();
        assertTrue(
                overriddenWorker.getSpec().getContainers().get(0).getCommand().stream()
                        .noneMatch(
                                argument ->
                                        argument.equals(
                                                "@/opt/seatunnel/config/jvm_worker_options")));
        assertTrue(
                overriddenWorker.getSpec().getContainers().get(0).getCommand().stream()
                        .anyMatch(
                                argument -> argument.equals("@/opt/seatunnel/config/jvm_options")));

        Map<String, String> disabledCommon = options();
        disabledCommon.put(KubernetesOptions.JVM_OPTIONS.key(), "disabled");
        KubernetesApplicationConfig commonDisabled =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(disabledCommon)));
        assertTrue(
                masterCommand(commonDisabled)
                        .contains("@/opt/seatunnel/config/jvm_master_options"));
        assertTrue(
                masterCommand(commonDisabled).stream()
                        .noneMatch(argument -> argument.endsWith("/jvm_options")));
    }

    @Test
    void rejectsInvalidJvmOptionsFiles() {
        Map<String, String> traversal = options();
        traversal.put(KubernetesOptions.MASTER_JVM_OPTIONS.key(), "../jvm_options");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(traversal))));
        Map<String, String> absoluteWithColon = options();
        absoluteWithColon.put(KubernetesOptions.WORKER_JVM_OPTIONS.key(), "/etc/seatunnel/a:b");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(absoluteWithColon))));
    }

    @Test
    void configuresTheSeatunnelConfigFile() {
        ApplicationSpecification specification = specification();
        KubernetesApplicationConfig defaults =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options())));
        assertTrue(
                masterCommand(defaults)
                        .contains("-Dseatunnel.config=/opt/seatunnel/config/seatunnel.yaml"));

        Map<String, String> relative = options();
        relative.put(KubernetesOptions.SEATUNNEL_CONFIG_FILE.key(), "custom/engine.yaml");
        assertTrue(
                masterCommand(
                                KubernetesApplicationConfig.from(
                                        specification,
                                        ReadonlyConfig.fromMap(new HashMap<>(relative))))
                        .contains("-Dseatunnel.config=/opt/seatunnel/custom/engine.yaml"));

        Map<String, String> absolute = options();
        absolute.put(KubernetesOptions.SEATUNNEL_CONFIG_FILE.key(), "/etc/seatunnel/engine.yaml");
        assertTrue(
                masterCommand(
                                KubernetesApplicationConfig.from(
                                        specification,
                                        ReadonlyConfig.fromMap(new HashMap<>(absolute))))
                        .contains("-Dseatunnel.config=/etc/seatunnel/engine.yaml"));
    }

    @Test
    void rejectsInvalidSeatunnelConfigFiles() {
        Map<String, String> disabled = options();
        disabled.put(KubernetesOptions.SEATUNNEL_CONFIG_FILE.key(), "disabled");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(disabled))));
        Map<String, String> traversal = options();
        traversal.put(KubernetesOptions.SEATUNNEL_CONFIG_FILE.key(), "../escape.yaml");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(traversal))));
        Map<String, String> wrongSuffix = options();
        wrongSuffix.put(KubernetesOptions.SEATUNNEL_CONFIG_FILE.key(), "config/engine.properties");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(wrongSuffix))));
    }

    @Test
    void configuresTheLog4jConfigFile() {
        ApplicationSpecification specification = specification();
        KubernetesApplicationConfig defaults =
                KubernetesApplicationConfig.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options())));
        assertTrue(
                masterCommand(defaults)
                        .contains(
                                "-Dlog4j2.configurationFile=/opt/seatunnel/config/log4j2_client.properties"));

        Map<String, String> serverConfig = options();
        serverConfig.put(KubernetesOptions.LOG4J_CONFIG_FILE.key(), "log4j2.properties");
        assertTrue(
                masterCommand(
                                KubernetesApplicationConfig.from(
                                        specification,
                                        ReadonlyConfig.fromMap(new HashMap<>(serverConfig))))
                        .contains(
                                "-Dlog4j2.configurationFile=/opt/seatunnel/config/log4j2.properties"));

        Map<String, String> invalid = options();
        invalid.put(KubernetesOptions.LOG4J_CONFIG_FILE.key(), "log4j2.conf");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(invalid))));

        Map<String, String> disabled = options();
        disabled.put(KubernetesOptions.LOG4J_CONFIG_FILE.key(), "disabled");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(disabled))));
    }

    @Test
    void buildsApplicationSelectorsFromTheOwnershipLabels() {
        assertEquals(
                KubernetesConstants.APPLICATION_LABEL + "=app",
                KubernetesResourceIdentity.selector("app"));
        assertEquals(
                KubernetesConstants.APPLICATION_LABEL
                        + "=app,"
                        + KubernetesConstants.ROLE_LABEL
                        + "=worker",
                KubernetesResourceIdentity.selector("app", KubernetesConstants.WORKER_ROLE));
    }

    @Test
    void mountsMultipleConfigMapsThroughSubPathsWithoutOverwritingEachOther() throws Exception {
        Map<String, String> options = options();
        options.put("kubernetes.configmap.first.ref", "platform-first");
        options.put(
                "kubernetes.configmap.first.mount.first.properties",
                "/opt/seatunnel/config/first.properties");
        options.put("kubernetes.configmap.second.name", "seatunnel-second");
        options.put("kubernetes.configmap.second.data.second.properties", "mode=batch");
        options.put(
                "kubernetes.configmap.second.mount.second.properties",
                "/opt/seatunnel/config/second.properties");
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        List<KubernetesResource<?>> application =
                KubernetesApplicationClusterDescriptor.assembleApplication(
                        "app", mainClass, parameters);
        V1Job job = applicationJob(application).getInternalResource();
        V1Volume referenced = configMapVolume(job, "platform-first");
        V1Volume created = configMapVolume(job, "seatunnel-second");
        assertEquals(
                "/opt/seatunnel/config/first.properties",
                subPath(job, referenced.getName(), "first.properties").getMountPath());
        assertEquals(
                "/opt/seatunnel/config/second.properties",
                subPath(job, created.getName(), "second.properties").getMountPath());
        KubernetesConfigMap createdModel =
                (KubernetesConfigMap)
                        accompanyingResources(application).stream()
                                .filter(KubernetesConfigMap.class::isInstance)
                                .findFirst()
                                .orElseThrow(() -> new AssertionError("Missing created ConfigMap"));
        assertEquals(
                "app",
                createdModel
                        .getInternalResource()
                        .getMetadata()
                        .getLabels()
                        .get(KubernetesConstants.APPLICATION_LABEL));
    }

    @Test
    void assembleApplicationReturnsTheOwnerJobBeforeItsDependencies() throws Exception {
        Map<String, String> options = options();
        options.put(KubernetesOptions.SECRET_NAME.key(), "job-secrets");
        options.put("kubernetes.secret.data.MYSQL_PASSWORD", "top-secret");
        options.put("kubernetes.configmap.runtime.data.seatunnel.yaml", "engine: {}\n");
        options.put(
                "kubernetes.configmap.runtime.mount.seatunnel.yaml",
                "/opt/seatunnel/config/seatunnel.yaml");
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        List<KubernetesResource<?>> application =
                KubernetesApplicationClusterDescriptor.assembleApplication(
                        "app", mainClass, parameters);
        // Owner Job first, exactly once, followed only by its dependencies.
        assertTrue(application.get(0) instanceof KubernetesJob);
        assertEquals(1, application.stream().filter(KubernetesJob.class::isInstance).count());
        assertTrue(
                accompanyingResources(application).stream()
                        .noneMatch(KubernetesJob.class::isInstance));
        assertEquals(1, application.stream().filter(KubernetesSecret.class::isInstance).count());
        assertEquals(1, application.stream().filter(KubernetesConfigMap.class::isInstance).count());
        assertEquals(1, application.stream().filter(KubernetesService.class::isInstance).count());
        // The caller owns the list and consumes the owner before creating the dependencies.
        KubernetesJob owner = (KubernetesJob) application.remove(0);
        assertEquals("app", owner.getName());
        assertEquals(3, application.size());
    }

    @Test
    void buildsHeadlessServiceForTheMasterPort() throws Exception {
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options())));
        List<KubernetesResource<?>> application =
                KubernetesApplicationClusterDescriptor.assembleApplication(
                        "app", mainClass, parameters);
        KubernetesService service =
                (KubernetesService)
                        accompanyingResources(application).stream()
                                .filter(KubernetesService.class::isInstance)
                                .findFirst()
                                .orElseThrow(() -> new AssertionError("Missing Service"));
        V1Service model = service.getInternalResource();
        assertEquals(KubernetesConstants.SERVICE_KIND, model.getKind());
        assertEquals("app", model.getMetadata().getName());
        assertEquals(
                "app", model.getMetadata().getLabels().get(KubernetesConstants.APPLICATION_LABEL));
        assertEquals(
                KubernetesConstants.MASTER_ROLE,
                model.getMetadata().getLabels().get(KubernetesConstants.ROLE_LABEL));
        // Headless: Kubernetes publishes the master Pod address directly, even before readiness.
        assertEquals(KubernetesConstants.HEADLESS_CLUSTER_IP, model.getSpec().getClusterIP());
        assertTrue(model.getSpec().getPublishNotReadyAddresses());
        assertEquals(
                "app", model.getSpec().getSelector().get(KubernetesConstants.APPLICATION_LABEL));
        assertEquals(
                KubernetesConstants.MASTER_ROLE,
                model.getSpec().getSelector().get(KubernetesConstants.ROLE_LABEL));
        int masterPort = specification().getMasterPort();
        V1ServicePort port = model.getSpec().getPorts().get(0);
        assertEquals(KubernetesConstants.HAZELCAST_PORT_NAME, port.getName());
        assertEquals(masterPort, port.getPort());
        assertEquals(masterPort, port.getTargetPort().getIntValue());
    }

    @Test
    void appliesTheConfiguredActiveDeadlineToTheOwnerJob() throws Exception {
        Map<String, String> options = options();
        options.put(KubernetesOptions.ACTIVE_DEADLINE_SECONDS.key(), "7200");
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        V1Job job =
                applicationJob(
                                KubernetesApplicationClusterDescriptor.assembleApplication(
                                        "app", mainClass, parameters))
                        .getInternalResource();
        assertEquals(7200L, job.getSpec().getActiveDeadlineSeconds());
        StringWriter writer = new StringWriter();
        parameters.write(writer);
        String encoded =
                Base64.getEncoder()
                        .encodeToString(writer.toString().getBytes(StandardCharsets.UTF_8));
        // The localized specification round-trips the deadline for the master's own settings.
        assertEquals(7200L, KubernetesApplicationConfig.read(encoded).getActiveDeadlineSeconds());
    }

    @Test
    void omitsTheActiveDeadlineByDefault() throws Exception {
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options())));
        V1Job job =
                applicationJob(
                                KubernetesApplicationClusterDescriptor.assembleApplication(
                                        "app", mainClass, parameters))
                        .getInternalResource();
        // Without the option a streaming application must stay active until it finishes.
        assertNull(job.getSpec().getActiveDeadlineSeconds());
    }

    @Test
    void rejectsNonPositiveActiveDeadlines() {
        Map<String, String> options = options();
        options.put(KubernetesOptions.ACTIVE_DEADLINE_SECONDS.key(), "0");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationConfig.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(options))));
    }

    @Test
    void rejectsUnknownKubernetesOptions() {
        Map<String, String> options = options();
        options.put("kubernetes.worker.toleration", "dedicated=seatunnel:NoSchedule");
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                KubernetesApplicationConfig.from(
                                        specification(),
                                        ReadonlyConfig.fromMap(new HashMap<>(options))));
        assertTrue(
                failure.getMessage().contains("kubernetes.worker.toleration"),
                failure.getMessage());
    }

    @Test
    void supportsEscapedCommasInAnnotationValues() throws Exception {
        Map<String, String> options = options();
        options.put(KubernetesOptions.WORKER_ANNOTATIONS.key(), "team:platform\\,batch");
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        assertEquals("platform,batch", parameters.getWorkerAnnotations().get("team"));

        StringWriter writer = new StringWriter();
        parameters.write(writer);
        String encoded =
                Base64.getEncoder()
                        .encodeToString(writer.toString().getBytes(StandardCharsets.UTF_8));
        assertEquals(
                "platform,batch",
                KubernetesApplicationConfig.read(encoded).getWorkerAnnotations().get("team"));
    }

    private static List<String> masterCommand(KubernetesApplicationConfig parameters) {
        return applicationJob(
                        KubernetesApplicationClusterDescriptor.assembleApplication(
                                "app", mainClass, parameters))
                .getInternalResource()
                .getSpec()
                .getTemplate()
                .getSpec()
                .getContainers()
                .get(0)
                .getCommand();
    }

    private static String envValue(V1Job job, String name) {
        for (V1EnvVar env : job.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv()) {
            if (name.equals(env.getName())) {
                return env.getValue();
            }
        }
        throw new AssertionError("Missing environment variable " + name);
    }

    private static V1EnvVar env(List<V1EnvVar> variables, String name) {
        return variables.stream()
                .filter(variable -> name.equals(variable.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing environment variable " + name));
    }

    private static V1Volume volume(V1Job job, String name) {
        for (V1Volume volume : job.getSpec().getTemplate().getSpec().getVolumes()) {
            if (name.equals(volume.getName())) {
                return volume;
            }
        }
        throw new AssertionError("Missing volume " + name);
    }

    private static V1Volume configMapVolume(V1Job job, String configMapName) {
        return job.getSpec().getTemplate().getSpec().getVolumes().stream()
                .filter(
                        volume ->
                                volume.getConfigMap() != null
                                        && configMapName.equals(volume.getConfigMap().getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing ConfigMap volume " + configMapName));
    }

    private static V1VolumeMount mount(V1Job job, String name) {
        for (V1VolumeMount mount :
                job.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts()) {
            if (name.equals(mount.getName())) {
                return mount;
            }
        }
        throw new AssertionError("Missing volume mount " + name);
    }

    private static V1VolumeMount subPath(V1Job job, String volumeName, String key) {
        for (V1VolumeMount mount :
                job.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts()) {
            if (volumeName.equals(mount.getName()) && key.equals(mount.getSubPath())) {
                return mount;
            }
        }
        throw new AssertionError("Missing subPath mount " + volumeName + "/" + key);
    }

    /** The assembled resource list carries the owner Job first, then its accompanying resources. */
    private static KubernetesJob applicationJob(List<KubernetesResource<?>> application) {
        return (KubernetesJob) application.get(0);
    }

    private static List<KubernetesResource<?>> accompanyingResources(
            List<KubernetesResource<?>> application) {
        return application.subList(1, application.size());
    }

    /**
     * Worker Pods are assembled by the resource manager driver from the application it belongs to,
     * so the test initializes a driver for the same identity before asking for one.
     */
    private static KubernetesPod workerPod(
            KubernetesJob job,
            String name,
            KubernetesApplicationConfig parameters,
            WorkerSpecification resources,
            String clusterName,
            String masterAddress)
            throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        when(api.getJob(job.getName())).thenReturn(job);
        try (KubernetesResourceManagerDriver driver =
                new KubernetesResourceManagerDriver(api, parameters, job.getName(), clusterName)) {
            driver.initialize(
                    mock(ResourceEventHandler.class),
                    mock(ScheduledExecutorService.class),
                    Runnable::run,
                    () -> masterAddress);
            return driver.assembleWorkerPod(name, resources);
        }
    }

    private static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put(KubernetesOptions.IMAGE.key(), "seatunnel:application");
        options.put(KubernetesOptions.KUBE_CONFIG.key(), "/submitter-kubeconfig");
        options.put(ApplicationOptions.WORKER_COUNT.key(), "2");
        return options;
    }
}
