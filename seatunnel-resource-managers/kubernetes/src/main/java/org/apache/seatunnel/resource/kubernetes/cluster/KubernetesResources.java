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

package org.apache.seatunnel.resource.kubernetes.cluster;

import org.apache.seatunnel.core.starter.seatunnel.application.ApplicationWorker;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.WorkerSpecification;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;
import org.apache.seatunnel.resource.kubernetes.KubernetesApplicationEntrypoint;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;

import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapVolumeSource;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobSpec;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Builds the owner Job and its dependent Kubernetes objects without making remote calls.
 *
 * <p>Each method returns a new mutable SDK model. The submitting caller owns that model and must
 * create or delete it through the SDK; creating a model alone allocates no cluster resources.
 */
public final class KubernetesResources {
    private static final String APPLICATION_LABEL = "seatunnel.apache.org/application-id";
    /** Identifies master and worker pods within one application. */
    public static final String ROLE_LABEL = "seatunnel.apache.org/role";

    static final String SPECIFICATION_FILE = "application.properties";
    private static final String CONFIG_DIRECTORY = "/etc/seatunnel-application";
    private static final String CHECKPOINT_DIRECTORY = "/opt/seatunnel/checkpoints";
    private static final String MASTER_HOST_ENV = "SEATUNNEL_APPLICATION_MASTER_HOST";

    private KubernetesResources() {}

    /**
     * Generates a fresh DNS-compatible Job name with room for generated pod suffixes.
     *
     * @param name application display name used as a sanitized prefix
     * @return a unique application identifier suitable for Kubernetes resource names
     */
    public static String newId(String name) {
        String prefix =
                name.toLowerCase(Locale.ROOT)
                        .replaceAll("[^a-z0-9-]", "-")
                        .replaceAll("^-+|-+$", "");
        if (prefix.isEmpty()) {
            prefix = "seatunnel";
        }
        prefix = prefix.substring(0, Math.min(prefix.length(), 35)).replaceAll("-+$", "");
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 12);
    }

    /**
     * Selects resources belonging to one application regardless of their role.
     *
     * @param id generated application Job name
     * @return equality selector for the application's ownership label
     */
    public static String selector(String id) {
        return APPLICATION_LABEL + "=" + id;
    }

    private static Map<String, String> labels(String id, String role) {
        Map<String, String> result = new HashMap<>();
        result.put(APPLICATION_LABEL, id);
        result.put(ROLE_LABEL, role);
        return result;
    }

    private static V1OwnerReference owner(V1Job job) {
        return new V1OwnerReference()
                .apiVersion("batch/v1")
                .kind("Job")
                .name(job.getMetadata().getName())
                .uid(job.getMetadata().getUid())
                .controller(false)
                .blockOwnerDeletion(false);
    }

    /**
     * Builds a suspended, non-restarting master Job with the configured terminal retention period.
     *
     * @param id generated application identifier
     * @param specification validated image, master resources and optional caller-owned PVC
     * @return Job model to create before its configuration and service; the caller starts it last
     */
    public static V1Job job(String id, ApplicationSpecification specification) {
        int memory = specification.getOption(ApplicationOptions.MASTER_MEMORY_MB);
        V1Container container =
                container(
                        specification,
                        memory,
                        specification.getOption(ApplicationOptions.MASTER_CPU_CORES));
        container.setCommand(
                command(
                        specification,
                        memory,
                        KubernetesApplicationEntrypoint.class.getName(),
                        id,
                        CONFIG_DIRECTORY + "/" + SPECIFICATION_FILE));
        container.addVolumeMountsItem(
                new V1VolumeMount().name("application").mountPath(CONFIG_DIRECTORY).readOnly(true));
        container.addEnvItem(
                new V1EnvVar()
                        .name(MASTER_HOST_ENV)
                        .valueFrom(
                                new V1EnvVarSource()
                                        .fieldRef(
                                                new V1ObjectFieldSelector()
                                                        .fieldPath("status.podIP"))));
        V1PodSpec pod =
                pod(specification, container, true)
                        .addVolumesItem(
                                new V1Volume()
                                        .name("application")
                                        .configMap(
                                                new V1ConfigMapVolumeSource()
                                                        .name(id)
                                                        .defaultMode(0444)));
        String checkpointClaim = specification.getOption(KubernetesOptions.CHECKPOINT_PVC);
        if (checkpointClaim != null) {
            container.addVolumeMountsItem(
                    new V1VolumeMount().name("checkpoints").mountPath(CHECKPOINT_DIRECTORY));
            pod.addVolumesItem(
                    new V1Volume()
                            .name("checkpoints")
                            .persistentVolumeClaim(
                                    new V1PersistentVolumeClaimVolumeSource()
                                            .claimName(checkpointClaim)
                                            .readOnly(false)));
        }
        return new V1Job()
                .apiVersion("batch/v1")
                .kind("Job")
                .metadata(new V1ObjectMeta().name(id).labels(labels(id, "master")))
                .spec(
                        new V1JobSpec()
                                .suspend(true)
                                .backoffLimit(0)
                                .completions(1)
                                .parallelism(1)
                                .ttlSecondsAfterFinished(
                                        specification.getOption(
                                                KubernetesOptions.RETENTION_SECONDS))
                                .template(
                                        new V1PodTemplateSpec()
                                                .metadata(
                                                        new V1ObjectMeta()
                                                                .labels(labels(id, "master")))
                                                .spec(pod)));
    }

    /**
     * Serializes the application into a ConfigMap owned by the already-created Job.
     *
     * @param job owner Job containing its server-assigned UID
     * @param specification immutable job content and deployment options
     * @return mounted configuration model with the submitter's local kubeconfig path removed
     * @throws IOException if temporary serialization or its guaranteed local cleanup fails
     */
    public static V1ConfigMap configMap(V1Job job, ApplicationSpecification specification)
            throws IOException {
        Path temporary = Files.createTempFile("seatunnel-kubernetes-", ".properties");
        String serialized;
        try {
            // A submitter's local kubeconfig path must never be used by the in-cluster driver.
            Map<String, String> options = new HashMap<>(specification.getOptions());
            options.remove(KubernetesOptions.KUBE_CONFIG.key());
            new ApplicationSpecification(
                            specification.getDeployType(),
                            specification.getName(),
                            specification.getJobConfig(),
                            specification.getWorkerCount(),
                            specification.getWorkerSpecification(),
                            options)
                    .write(temporary);
            serialized = new String(Files.readAllBytes(temporary), StandardCharsets.UTF_8);
        } finally {
            Files.deleteIfExists(temporary);
        }
        return new V1ConfigMap()
                .apiVersion("v1")
                .kind("ConfigMap")
                .metadata(metadata(job, job.getMetadata().getName(), "configuration"))
                .data(Collections.singletonMap(SPECIFICATION_FILE, serialized));
    }

    /**
     * Builds the headless service owned by the application's master Job.
     *
     * @param job owner Job containing its server-assigned UID
     * @param specification application master network options
     * @return service model selecting only this application's master pod
     */
    public static V1Service service(V1Job job, ApplicationSpecification specification) {
        String id = job.getMetadata().getName();
        int port = specification.getOption(ApplicationOptions.MASTER_PORT);
        return new V1Service()
                .apiVersion("v1")
                .kind("Service")
                .metadata(metadata(job, id, "master"))
                .spec(
                        new V1ServiceSpec()
                                .clusterIP("None")
                                .publishNotReadyAddresses(true)
                                .selector(labels(id, "master"))
                                .addPortsItem(
                                        new V1ServicePort()
                                                .name("hazelcast")
                                                .port(port)
                                                .targetPort(new IntOrString(port))));
    }

    static V1Pod worker(
            V1Job job,
            String name,
            ApplicationSpecification specification,
            WorkerSpecification resources,
            String clusterName,
            String masterAddress) {
        V1Container container =
                container(specification, resources.getMemoryMb(), resources.getCpuCores());
        container.setCommand(
                command(
                        specification,
                        resources.getMemoryMb(),
                        ApplicationWorker.class.getName(),
                        clusterName,
                        masterAddress,
                        Integer.toString(resources.getSlots())));
        return new V1Pod()
                .apiVersion("v1")
                .kind("Pod")
                .metadata(metadata(job, name, "worker"))
                .spec(pod(specification, container, false));
    }

    private static V1ObjectMeta metadata(V1Job job, String name, String role) {
        return new V1ObjectMeta()
                .name(name)
                .labels(labels(job.getMetadata().getName(), role))
                .ownerReferences(Collections.singletonList(owner(job)));
    }

    private static V1PodSpec pod(
            ApplicationSpecification specification, V1Container container, boolean token) {
        return new V1PodSpec()
                .restartPolicy("Never")
                .terminationGracePeriodSeconds(120L)
                .serviceAccountName(specification.getOption(KubernetesOptions.SERVICE_ACCOUNT))
                .automountServiceAccountToken(token)
                .containers(Collections.singletonList(container));
    }

    private static V1Container container(
            ApplicationSpecification specification, int memory, int cpu) {
        Map<String, Quantity> resources = new HashMap<>();
        resources.put("memory", Quantity.fromString(memory + "Mi"));
        resources.put("cpu", Quantity.fromString(Integer.toString(cpu)));
        String home = specification.getOption(KubernetesOptions.SEATUNNEL_HOME);
        return new V1Container()
                .name("seatunnel")
                .image(specification.getOption(KubernetesOptions.IMAGE))
                .imagePullPolicy(specification.getOption(KubernetesOptions.IMAGE_PULL_POLICY))
                .workingDir(home)
                .addEnvItem(new V1EnvVar().name("SEATUNNEL_HOME").value(home))
                .resources(
                        new V1ResourceRequirements()
                                .requests(resources)
                                .limits(new HashMap<>(resources)));
    }

    private static List<String> command(
            ApplicationSpecification specification, int memory, String main, String... args) {
        String home = specification.getOption(KubernetesOptions.SEATUNNEL_HOME);
        List<String> command =
                new ArrayList<>(
                        Arrays.asList(
                                "java",
                                "-Xmx" + Math.max(1, memory * 3L / 4) + "m",
                                "-Dseatunnel.home=" + home,
                                "-Dseatunnel.config=" + home + "/config/seatunnel.yaml",
                                "-Dlog4j2.configurationFile="
                                        + home
                                        + "/config/log4j2_client.properties",
                                "-cp",
                                home
                                        + "/starter/seatunnel-starter.jar:"
                                        + home
                                        + "/starter/logging/*:"
                                        + home
                                        + "/lib/*:"
                                        + home
                                        + "/resource-managers/kubernetes/*:"
                                        + home
                                        + "/config",
                                main));
        command.addAll(Arrays.asList(args));
        return command;
    }
}
