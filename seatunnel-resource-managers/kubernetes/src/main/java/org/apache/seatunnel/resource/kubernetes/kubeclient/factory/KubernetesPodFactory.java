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

import org.apache.seatunnel.core.starter.seatunnel.application.ApplicationWorker;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.WorkerSpecification;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;
import org.apache.seatunnel.resource.kubernetes.KubernetesApplicationEntrypoint;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;

import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1ConfigMapVolumeSource;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1ExecAction;
import io.kubernetes.client.openapi.models.V1LocalObjectReference;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Probe;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1SecretVolumeSource;
import io.kubernetes.client.openapi.models.V1TCPSocketAction;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Builds master and worker pod specifications without creating Kubernetes resources. */
final class KubernetesPodFactory {
    private KubernetesPodFactory() {}

    /**
     * Builds the application master pod, including its private specification and optional
     * checkpoint volumes.
     *
     * @param id application resource name and specification Secret name
     * @param parameters validated application and Kubernetes settings
     * @return pod specification embedded in the owner Job
     */
    static V1PodSpec master(String id, KubernetesApplicationParameters parameters) {
        ApplicationSpecification specification = parameters.getSpecification();
        int memory = specification.getOption(ApplicationOptions.MASTER_MEMORY_MB);
        V1Container container =
                container(
                        parameters,
                        memory,
                        specification.getOption(ApplicationOptions.MASTER_CPU_CORES));
        container.setCommand(
                command(
                        parameters,
                        memory,
                        KubernetesApplicationEntrypoint.class.getName(),
                        id,
                        KubernetesConstants.CONFIG_DIRECTORY
                                + "/"
                                + KubernetesConstants.SPECIFICATION_FILE));
        container.addVolumeMountsItem(
                new V1VolumeMount()
                        .name(KubernetesConstants.APPLICATION_VOLUME)
                        .mountPath(KubernetesConstants.CONFIG_DIRECTORY)
                        .readOnly(true));
        container.addEnvItem(
                new V1EnvVar()
                        .name(KubernetesConstants.MASTER_HOST_ENV)
                        .valueFrom(
                                new V1EnvVarSource()
                                        .fieldRef(
                                                new V1ObjectFieldSelector()
                                                        .fieldPath(
                                                                KubernetesConstants
                                                                        .POD_IP_FIELD_PATH))));
        addMasterProbes(
                container, specification.getOption(ApplicationOptions.MASTER_PORT), specification);
        V1PodSpec pod =
                pod(parameters, container, true)
                        .addVolumesItem(
                                new V1Volume()
                                        .name(KubernetesConstants.APPLICATION_VOLUME)
                                        .secret(
                                                new V1SecretVolumeSource()
                                                        .secretName(id)
                                                        .defaultMode(
                                                                KubernetesConstants
                                                                        .APPLICATION_SECRET_MODE)));
        mountRuntimeConfiguration(parameters, container, pod);
        String checkpointClaim = parameters.getCheckpointPvc();
        if (checkpointClaim != null) {
            container.addVolumeMountsItem(
                    new V1VolumeMount()
                            .name(KubernetesConstants.CHECKPOINT_VOLUME)
                            .mountPath(KubernetesConstants.CHECKPOINT_DIRECTORY));
            pod.addVolumesItem(
                    new V1Volume()
                            .name(KubernetesConstants.CHECKPOINT_VOLUME)
                            .persistentVolumeClaim(
                                    new V1PersistentVolumeClaimVolumeSource()
                                            .claimName(checkpointClaim)
                                            .readOnly(false)));
        }
        return pod;
    }

    /**
     * Builds a worker pod that joins one application master and exposes fixed task slots.
     *
     * @param parameters validated application and Kubernetes settings
     * @param resources per-worker resource and slot settings
     * @param clusterName isolated Hazelcast cluster name
     * @param masterAddress advertised application master address
     * @return standalone worker pod specification without Kubernetes API credentials
     */
    static V1PodSpec worker(
            KubernetesApplicationParameters parameters,
            WorkerSpecification resources,
            String clusterName,
            String masterAddress) {
        V1Container container =
                container(parameters, resources.getMemoryMb(), resources.getCpuCores());
        container.setCommand(
                command(
                        parameters,
                        resources.getMemoryMb(),
                        ApplicationWorker.class.getName(),
                        clusterName,
                        masterAddress,
                        Integer.toString(resources.getSlots())));
        container.setLivenessProbe(processProbe());
        V1PodSpec pod = pod(parameters, container, false);
        mountRuntimeConfiguration(parameters, container, pod);
        return pod;
    }

    private static void mountRuntimeConfiguration(
            KubernetesApplicationParameters parameters, V1Container container, V1PodSpec pod) {
        if (parameters.getConfigMap() == null) {
            return;
        }
        container.addVolumeMountsItem(
                new V1VolumeMount()
                        .name(KubernetesConstants.CONFIG_VOLUME)
                        .mountPath(parameters.getSeatunnelHome() + "/config")
                        .readOnly(true));
        pod.addVolumesItem(
                new V1Volume()
                        .name(KubernetesConstants.CONFIG_VOLUME)
                        .configMap(new V1ConfigMapVolumeSource().name(parameters.getConfigMap())));
    }

    private static V1PodSpec pod(
            KubernetesApplicationParameters parameters, V1Container container, boolean apiToken) {
        V1PodSpec pod =
                new V1PodSpec()
                        .restartPolicy(KubernetesConstants.RESTART_POLICY_NEVER)
                        .terminationGracePeriodSeconds(
                                KubernetesConstants.TERMINATION_GRACE_PERIOD_SECONDS)
                        .serviceAccountName(parameters.getServiceAccount())
                        .automountServiceAccountToken(apiToken)
                        .containers(Collections.singletonList(container));
        for (String secret : parameters.getImagePullSecrets()) {
            pod.addImagePullSecretsItem(new V1LocalObjectReference().name(secret));
        }
        pod.setNodeSelector(
                apiToken ? parameters.getMasterNodeSelector() : parameters.getWorkerNodeSelector());
        return pod;
    }

    private static V1Container container(
            KubernetesApplicationParameters parameters, int memory, int cpu) {
        Map<String, Quantity> resources = new HashMap<>();
        resources.put(
                KubernetesConstants.MEMORY_RESOURCE,
                Quantity.fromString(memory + KubernetesConstants.MEBIBYTE_SUFFIX));
        resources.put(KubernetesConstants.CPU_RESOURCE, Quantity.fromString(Integer.toString(cpu)));
        String home = parameters.getSeatunnelHome();
        return new V1Container()
                .name(KubernetesConstants.CONTAINER_NAME)
                .image(parameters.getImage())
                .imagePullPolicy(parameters.getImagePullPolicy())
                .workingDir(home)
                .addEnvItem(new V1EnvVar().name(KubernetesConstants.SEATUNNEL_HOME_ENV).value(home))
                .resources(
                        new V1ResourceRequirements()
                                .requests(resources)
                                .limits(new HashMap<>(resources)));
    }

    private static List<String> command(
            KubernetesApplicationParameters parameters, int memory, String main, String... args) {
        String home = parameters.getSeatunnelHome();
        List<String> command =
                new ArrayList<>(
                        Arrays.asList(
                                KubernetesConstants.JAVA_COMMAND,
                                "-Xmx"
                                        + Math.max(
                                                KubernetesConstants.MINIMUM_JVM_HEAP_MB,
                                                memory
                                                        * (long)
                                                                KubernetesConstants
                                                                        .JVM_HEAP_NUMERATOR
                                                        / KubernetesConstants.JVM_HEAP_DENOMINATOR)
                                        + "m",
                                "-Dseatunnel.home=" + home,
                                "-Dseatunnel.config="
                                        + home
                                        + KubernetesConstants.SEATUNNEL_CONFIG_FILE,
                                "-Dlog4j2.configurationFile="
                                        + home
                                        + KubernetesConstants.LOG4J_CONFIG_FILE,
                                "-cp",
                                home
                                        + String.format(
                                                KubernetesConstants.KUBERNETES_CLASSPATH,
                                                home,
                                                home,
                                                home,
                                                home),
                                main));
        command.addAll(Arrays.asList(args));
        return command;
    }

    private static void addMasterProbes(
            V1Container container, int port, ApplicationSpecification specification) {
        V1Probe tcpProbe = tcpProbe(port);
        int startupFailures =
                Math.max(
                        1,
                        (int)
                                Math.ceil(
                                        specification.getStartupTimeoutMillis()
                                                / (double)
                                                        KubernetesConstants
                                                                .STARTUP_PROBE_PERIOD_MILLIS));
        container.setStartupProbe(
                tcpProbe(port)
                        .periodSeconds(KubernetesConstants.STARTUP_PROBE_PERIOD_MILLIS / 1000)
                        .failureThreshold(startupFailures));
        container.setReadinessProbe(tcpProbe);
        container.setLivenessProbe(tcpProbe(port));
    }

    private static V1Probe tcpProbe(int port) {
        return new V1Probe()
                .tcpSocket(new V1TCPSocketAction().port(new IntOrString(port)))
                .timeoutSeconds(KubernetesConstants.PROBE_TIMEOUT_SECONDS)
                .periodSeconds(KubernetesConstants.PROBE_PERIOD_SECONDS)
                .failureThreshold(KubernetesConstants.PROBE_FAILURE_THRESHOLD);
    }

    private static V1Probe processProbe() {
        return new V1Probe()
                .exec(
                        new V1ExecAction()
                                .command(
                                        Arrays.asList(
                                                KubernetesConstants.SHELL_COMMAND,
                                                "-c",
                                                KubernetesConstants.PROCESS_PROBE_COMMAND)))
                .timeoutSeconds(KubernetesConstants.PROBE_TIMEOUT_SECONDS)
                .periodSeconds(KubernetesConstants.PROBE_PERIOD_SECONDS)
                .failureThreshold(KubernetesConstants.PROBE_FAILURE_THRESHOLD);
    }
}
