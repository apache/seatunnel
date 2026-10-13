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

package org.apache.seatunnel.resource.kubernetes.kubeclient.factory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.resource.kubernetes.cli.SeatunnelKubernetesApplicationCli;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesApplicationSpecification;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesSecret;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.seatunnel.resource.kubernetes.worker.SeatunnelKubernetesApplicationWorker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KubernetesResourceFactoryTest {

    @TempDir Path temporary;

    private static final String mainClass = SeatunnelKubernetesApplicationCli.class.getName();

    @ParameterizedTest
    @ValueSource(strings = {"Always", "IfNotPresent", "Never"})
    void preservesKubernetesImagePullPolicies(String policy) {
        Map<String, String> options = options();
        options.put(KubernetesOptions.IMAGE_PULL_POLICY.key(), policy);
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        assertEquals(policy, parameters.getImagePullPolicy());
        assertEquals(
                policy,
                KubernetesResourceFactory.job("app", mainClass, parameters)
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
                                KubernetesApplicationParameters.from(
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
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        KubernetesJob job =
                KubernetesApplicationFactory.buildApplicationSpecification(
                                "application", mainClass, parameters)
                        .getApplicationJob();
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
                KubernetesResourceFactory.worker(
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
        options.put("kubernetes.configmap.runtime.ref", "seatunnel-runtime");
        options.put(
                "kubernetes.configmap.runtime.mount.seatunnel.yaml",
                "/opt/seatunnel/config/seatunnel.yaml");
        options.put(KubernetesOptions.NAMESPACE.key(), "analytics");
        options.put("submitter.private-setting", "must-not-be-localized");
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env {}", options);
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        KubernetesJob job =
                KubernetesApplicationFactory.buildApplicationSpecification(
                                "app", mainClass, parameters)
                        .getApplicationJob();
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
        KubernetesApplicationParameters restored =
                KubernetesApplicationParameters.read(encodedSpecification);
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
        String runtimeVolume = KubernetesConstants.CONFIG_VOLUME + "-0";
        assertEquals(
                "seatunnel-runtime", volume(jobResource, runtimeVolume).getConfigMap().getName());
        // Mounting one key through subPath keeps the rest of the image's configuration directory.
        assertEquals(
                "/opt/seatunnel/config/seatunnel.yaml",
                mount(jobResource, runtimeVolume).getMountPath());
        assertEquals("seatunnel.yaml", mount(jobResource, runtimeVolume).getSubPath());
        assertTrue(mount(jobResource, runtimeVolume).getReadOnly());
        KubernetesService service = KubernetesResourceFactory.service("app", parameters);
        assertEquals("None", service.getInternalResource().getSpec().getClusterIP());
        KubernetesPod workerPod =
                KubernetesResourceFactory.worker(
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
                "worker",
                worker.getMetadata().getLabels().get(KubernetesResourceFactory.ROLE_LABEL));
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
        assertEquals(
                "kill -0 1",
                worker.getSpec()
                        .getContainers()
                        .get(0)
                        .getLivenessProbe()
                        .getExec()
                        .getCommand()
                        .get(2));
    }

    private static ApplicationSpecification specification() {
        return SeatunnelApplicationConfig.parse("env { job.mode = BATCH }", options());
    }

    @Test
    void mountsReferencedSecretWithoutCreatingResources() throws Exception {
        Map<String, String> options = options();
        options.put(KubernetesOptions.SECRET_REF.key(), "platform-secret");
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        KubernetesApplicationSpecification application =
                KubernetesApplicationFactory.buildApplicationSpecification(
                        "app", mainClass, parameters);
        assertTrue(
                application.getAccompanyingResources().stream()
                        .noneMatch(KubernetesSecret.class::isInstance));
        V1Job job = application.getApplicationJob().getInternalResource();
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
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        KubernetesApplicationSpecification application =
                KubernetesApplicationFactory.buildApplicationSpecification(
                        "app", mainClass, parameters);
        assertEquals(2, application.getAccompanyingResources().size());
        KubernetesResource<?> configurationSecret =
                application.getAccompanyingResources().stream()
                        .filter(KubernetesSecret.class::isInstance)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Missing created Secret"));
        V1Secret secret = ((KubernetesSecret) configurationSecret).getInternalResource();
        assertEquals("job-secrets", secret.getMetadata().getName());
        assertEquals("Opaque", secret.getType());
        assertEquals("top-secret", secret.getStringData().get("MYSQL_PASSWORD"));
        assertNull(secret.getMetadata().getOwnerReferences());
        V1Job job = application.getApplicationJob().getInternalResource();
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
                        KubernetesApplicationParameters.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(conflicting))));
        Map<String, String> nameOnly = options();
        nameOnly.put(KubernetesOptions.SECRET_NAME.key(), "job-secrets");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationParameters.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(nameOnly))));
    }

    @Test
    void injectsDeclaredEnvironmentVariablesIntoMasterAndWorkerPods() throws Exception {
        Map<String, String> options = options();
        options.put(
                KubernetesOptions.ENV.key(),
                "LITERAL=value,FROM_SECRET=secret:db-secret/password,FROM_CONFIG=configmap:runtime-cm/path");
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        KubernetesApplicationSpecification application =
                KubernetesApplicationFactory.buildApplicationSpecification(
                        "app", mainClass, parameters);
        KubernetesJob applicationJob = application.getApplicationJob();
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
        V1Pod worker =
                KubernetesResourceFactory.worker(
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
    }

    @Test
    void rejectsInvalidEnvironmentVariableEntries() {
        Map<String, String> reserved = options();
        reserved.put(KubernetesOptions.ENV.key(), "SEATUNNEL_APPLICATION_SPECIFICATION=value");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationParameters.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(reserved))));
        Map<String, String> malformed = options();
        malformed.put(KubernetesOptions.ENV.key(), "MISSING_SEPARATOR");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationParameters.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(malformed))));
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
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        KubernetesApplicationSpecification application =
                KubernetesApplicationFactory.buildApplicationSpecification(
                        "app", mainClass, parameters);
        KubernetesResource<?> configuration =
                application.getAccompanyingResources().stream()
                        .filter(KubernetesConfigMap.class::isInstance)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Missing created ConfigMap"));
        V1ConfigMap configMap = ((KubernetesConfigMap) configuration).getInternalResource();
        assertEquals("app-runtime", configMap.getMetadata().getName());
        assertEquals("engine:\n  job: {}\n", configMap.getData().get("seatunnel.yaml"));
        V1Job job = application.getApplicationJob().getInternalResource();
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
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        KubernetesApplicationSpecification application =
                KubernetesApplicationFactory.buildApplicationSpecification(
                        "app", mainClass, parameters);
        assertTrue(
                application.getAccompanyingResources().stream()
                        .noneMatch(KubernetesConfigMap.class::isInstance));
        V1Job job = application.getApplicationJob().getInternalResource();
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
                        KubernetesApplicationParameters.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(conflicting))));
        Map<String, String> referenceWithoutMount = options();
        referenceWithoutMount.put("kubernetes.configmap.shared.ref", "platform-runtime");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationParameters.from(
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
                        KubernetesApplicationParameters.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(unknownMount))));
        Map<String, String> missingMount = options();
        missingMount.put("kubernetes.configmap.runtime.data.seatunnel.yaml", "content");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationParameters.from(
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
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        KubernetesApplicationSpecification application =
                KubernetesApplicationFactory.buildApplicationSpecification(
                        "app", mainClass, parameters);
        KubernetesResource<?> configuration =
                application.getAccompanyingResources().stream()
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
                "seatunnel-generated-config",
                volume(
                                application.getApplicationJob().getInternalResource(),
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
                        KubernetesApplicationParameters.from(
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
                        KubernetesApplicationParameters.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(invalidName))));
        Map<String, String> nameWithoutData = options();
        nameWithoutData.put("kubernetes.configmap.generated.name", "seatunnel-generated-config");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationParameters.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(nameWithoutData))));
    }

    @Test
    void replacesInlineJobConfigurationWithMountedOverride() throws Exception {
        Path secretDirectory = temporary.resolve("secrets");
        Files.createDirectories(secretDirectory);
        Files.write(
                secretDirectory.resolve("job.conf"),
                "env { job.mode = STREAMING }".getBytes(StandardCharsets.UTF_8));
        Map<String, String> options = options();
        options.put(KubernetesOptions.SECRET_DIRECTORY.key(), secretDirectory.toString());
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env { job.mode = BATCH }", options);
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        StringWriter writer = new StringWriter();
        parameters.write(writer);
        String encoded =
                Base64.getEncoder()
                        .encodeToString(writer.toString().getBytes(StandardCharsets.UTF_8));
        KubernetesApplicationParameters restored = KubernetesApplicationParameters.read(encoded);
        assertEquals("env { job.mode = STREAMING }", restored.getSpecification().getJobConfig());
    }

    @Test
    void usesConfiguredMountDirectories() throws Exception {
        Map<String, String> options = options();
        options.put(KubernetesOptions.SECRET_DIRECTORY.key(), "/var/run/seatunnel/secrets");
        options.put(KubernetesOptions.CHECKPOINT_DIRECTORY.key(), "/var/run/seatunnel/checkpoints");
        options.put(KubernetesOptions.CHECKPOINT_PVC.key(), "existing-checkpoints");
        options.put(KubernetesOptions.SECRET_REF.key(), "platform-secret");
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        KubernetesApplicationSpecification application =
                KubernetesApplicationFactory.buildApplicationSpecification(
                        "app", mainClass, parameters);
        V1Job job = application.getApplicationJob().getInternalResource();
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
        KubernetesApplicationParameters restored = KubernetesApplicationParameters.read(encoded);
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
                        KubernetesApplicationParameters.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(relativeSecret))));
        Map<String, String> relativeCheckpoint = options();
        relativeCheckpoint.put(KubernetesOptions.CHECKPOINT_DIRECTORY.key(), "checkpoints");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationParameters.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(relativeCheckpoint))));
    }

    @Test
    void buildsClasspathFromTheConfiguredSeatunnelHome() {
        Map<String, String> options = options();
        options.put(KubernetesOptions.SEATUNNEL_HOME.key(), "/custom/seatunnel");
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        KubernetesJob job = KubernetesResourceFactory.job("app", mainClass, parameters);
        List<String> command =
                job.getInternalResource()
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand();
        assertEquals(
                "/custom/seatunnel/starter/seatunnel-starter.jar:"
                        + "/custom/seatunnel/starter/logging/*:"
                        + "/custom/seatunnel/lib/*:"
                        + "/custom/seatunnel/resource-managers/kubernetes/*:"
                        + "/custom/seatunnel/config",
                command.get(command.indexOf("-cp") + 1));
    }

    @Test
    void addsRoleSpecificHazelcastConfiguration() {
        Map<String, String> options = options();
        options.put(KubernetesOptions.MASTER_HAZELCAST_CONFIG.key(), "hazelcast-master.yaml");
        options.put(
                KubernetesOptions.WORKER_HAZELCAST_CONFIG.key(),
                "/etc/seatunnel/hazelcast-worker.yaml");
        ApplicationSpecification specification = specification();
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
        KubernetesJob job = KubernetesResourceFactory.job("app", mainClass, parameters);
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
                KubernetesResourceFactory.worker(
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
                        KubernetesApplicationParameters.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(invalidSuffix))));
        Map<String, String> traversal = options();
        traversal.put(KubernetesOptions.WORKER_HAZELCAST_CONFIG.key(), "../hazelcast-worker.yaml");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationParameters.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(traversal))));
    }

    @Test
    void appliesJvmOptionsFilesAndMandatoryModuleFlags() {
        ApplicationSpecification specification = specification();
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options())));
        KubernetesJob job = KubernetesResourceFactory.job("app", mainClass, parameters);
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
                KubernetesResourceFactory.worker(
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
        overrides.put(KubernetesOptions.WORKER_JVM_OPTIONS.key(), "none");
        KubernetesApplicationParameters overridden =
                KubernetesApplicationParameters.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(overrides)));
        KubernetesJob overriddenJob = KubernetesResourceFactory.job("app", mainClass, overridden);
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
                KubernetesResourceFactory.worker(
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
        disabledCommon.put(KubernetesOptions.JVM_OPTIONS.key(), "none");
        KubernetesApplicationParameters commonDisabled =
                KubernetesApplicationParameters.from(
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
                        KubernetesApplicationParameters.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(traversal))));
        Map<String, String> absoluteWithColon = options();
        absoluteWithColon.put(KubernetesOptions.WORKER_JVM_OPTIONS.key(), "/etc/seatunnel/a:b");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationParameters.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(absoluteWithColon))));
    }

    @Test
    void configuresTheSeatunnelConfigFile() {
        ApplicationSpecification specification = specification();
        KubernetesApplicationParameters defaults =
                KubernetesApplicationParameters.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options())));
        assertTrue(
                masterCommand(defaults)
                        .contains("-Dseatunnel.config=/opt/seatunnel/config/seatunnel.yaml"));

        Map<String, String> relative = options();
        relative.put(KubernetesOptions.SEATUNNEL_CONFIG_FILE.key(), "custom/engine.yaml");
        assertTrue(
                masterCommand(
                                KubernetesApplicationParameters.from(
                                        specification,
                                        ReadonlyConfig.fromMap(new HashMap<>(relative))))
                        .contains("-Dseatunnel.config=/opt/seatunnel/custom/engine.yaml"));

        Map<String, String> absolute = options();
        absolute.put(KubernetesOptions.SEATUNNEL_CONFIG_FILE.key(), "/etc/seatunnel/engine.yaml");
        assertTrue(
                masterCommand(
                                KubernetesApplicationParameters.from(
                                        specification,
                                        ReadonlyConfig.fromMap(new HashMap<>(absolute))))
                        .contains("-Dseatunnel.config=/etc/seatunnel/engine.yaml"));

        Map<String, String> disabled = options();
        disabled.put(KubernetesOptions.SEATUNNEL_CONFIG_FILE.key(), "none");
        assertTrue(
                masterCommand(
                                KubernetesApplicationParameters.from(
                                        specification,
                                        ReadonlyConfig.fromMap(new HashMap<>(disabled))))
                        .stream()
                        .noneMatch(argument -> argument.startsWith("-Dseatunnel.config=")));
    }

    @Test
    void rejectsInvalidSeatunnelConfigFiles() {
        Map<String, String> traversal = options();
        traversal.put(KubernetesOptions.SEATUNNEL_CONFIG_FILE.key(), "../escape.yaml");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationParameters.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(traversal))));
        Map<String, String> wrongSuffix = options();
        wrongSuffix.put(KubernetesOptions.SEATUNNEL_CONFIG_FILE.key(), "config/engine.properties");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationParameters.from(
                                specification(),
                                ReadonlyConfig.fromMap(new HashMap<>(wrongSuffix))));
    }

    @Test
    void configuresTheLog4jConfigFile() {
        ApplicationSpecification specification = specification();
        KubernetesApplicationParameters defaults =
                KubernetesApplicationParameters.from(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options())));
        assertTrue(
                masterCommand(defaults)
                        .contains(
                                "-Dlog4j2.configurationFile=/opt/seatunnel/config/log4j2_client.properties"));

        Map<String, String> serverConfig = options();
        serverConfig.put(KubernetesOptions.LOG4J_CONFIG_FILE.key(), "log4j2.properties");
        assertTrue(
                masterCommand(
                                KubernetesApplicationParameters.from(
                                        specification,
                                        ReadonlyConfig.fromMap(new HashMap<>(serverConfig))))
                        .contains(
                                "-Dlog4j2.configurationFile=/opt/seatunnel/config/log4j2.properties"));

        Map<String, String> disabled = options();
        disabled.put(KubernetesOptions.LOG4J_CONFIG_FILE.key(), "none");
        assertTrue(
                masterCommand(
                                KubernetesApplicationParameters.from(
                                        specification,
                                        ReadonlyConfig.fromMap(new HashMap<>(disabled))))
                        .stream()
                        .noneMatch(argument -> argument.startsWith("-Dlog4j2.configurationFile=")));

        Map<String, String> invalid = options();
        invalid.put(KubernetesOptions.LOG4J_CONFIG_FILE.key(), "log4j2.conf");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesApplicationParameters.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(invalid))));
    }

    private static List<String> masterCommand(KubernetesApplicationParameters parameters) {
        return KubernetesResourceFactory.job("app", mainClass, parameters)
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

    private static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put(KubernetesOptions.IMAGE.key(), "seatunnel:application");
        options.put(KubernetesOptions.KUBE_CONFIG.key(), "/submitter-kubeconfig");
        options.put(ApplicationOptions.WORKER_COUNT.key(), "2");
        return options;
    }
}
