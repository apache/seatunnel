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

import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.worker.SeatunnelKubernetesApplicationWorker;

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
    static V1PodSpec master(
            String id, String mainClass, KubernetesApplicationParameters parameters) {
        ApplicationSpecification specification = parameters.getSpecification();
        int memory = specification.getMasterMemoryMb();
        V1Container container = container(parameters, memory, specification.getMasterCpuCores());
        container.setCommand(
                command(
                        parameters,
                        memory,
                        mainClass,
                        id,
                        KubernetesConstants.CONFIG_DIRECTORY
                                + "/"
                                + KubernetesConstants.SPECIFICATION_FILE));
        container.addVolumeMountsItem(
                volumeMount(
                        KubernetesConstants.APPLICATION_VOLUME,
                        KubernetesConstants.CONFIG_DIRECTORY,
                        true));
        container.addEnvItem(masterHostEnv());
        addMasterProbes(container, specification.getMasterPort(), specification);
        V1PodSpec pod = pod(parameters, container, true).addVolumesItem(applicationVolume(id));
        mountRuntimeConfiguration(parameters, container, pod);
        String checkpointClaim = parameters.getCheckpointPvc();
        if (checkpointClaim != null) {
            container.addVolumeMountsItem(
                    volumeMount(
                            KubernetesConstants.CHECKPOINT_VOLUME,
                            KubernetesConstants.CHECKPOINT_DIRECTORY,
                            false));
            pod.addVolumesItem(checkpointVolume(checkpointClaim));
        }
        return pod;
    }

    private static V1EnvVar masterHostEnv() {
        return new V1EnvVar()
                .name(KubernetesConstants.MASTER_HOST_ENV)
                .valueFrom(
                        new V1EnvVarSource()
                                .fieldRef(
                                        new V1ObjectFieldSelector()
                                                .fieldPath(KubernetesConstants.POD_IP_FIELD_PATH)));
    }

    private static V1Volume applicationVolume(String id) {
        return new V1Volume()
                .name(KubernetesConstants.APPLICATION_VOLUME)
                .secret(
                        new V1SecretVolumeSource()
                                .secretName(id)
                                .defaultMode(KubernetesConstants.APPLICATION_SECRET_MODE));
    }

    private static V1Volume checkpointVolume(String claim) {
        return new V1Volume()
                .name(KubernetesConstants.CHECKPOINT_VOLUME)
                .persistentVolumeClaim(
                        new V1PersistentVolumeClaimVolumeSource().claimName(claim).readOnly(false));
    }

    private static V1VolumeMount volumeMount(String name, String path, boolean readOnly) {
        return new V1VolumeMount().name(name).mountPath(path).readOnly(readOnly);
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
                        SeatunnelKubernetesApplicationWorker.class.getName(),
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
                volumeMount(
                        KubernetesConstants.CONFIG_VOLUME,
                        parameters.getSeatunnelHome() + "/config",
                        true));
        pod.addVolumesItem(configMapVolume(parameters.getConfigMap()));
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
        String home = parameters.getSeatunnelHome();
        V1ResourceRequirements requirements = resourceRequirements(memory, cpu);
        return new V1Container()
                .name(KubernetesConstants.CONTAINER_NAME)
                .image(parameters.getImage())
                .imagePullPolicy(parameters.getImagePullPolicy())
                .workingDir(home)
                .addEnvItem(homeEnv(home))
                .resources(requirements);
    }

    private static V1EnvVar homeEnv(String home) {
        return new V1EnvVar().name(KubernetesConstants.SEATUNNEL_HOME_ENV).value(home);
    }

    private static V1ResourceRequirements resourceRequirements(int memory, int cpu) {
        Map<String, Quantity> resources = new HashMap<>();
        resources.put(
                KubernetesConstants.MEMORY_RESOURCE,
                Quantity.fromString(memory + KubernetesConstants.MEBIBYTE_SUFFIX));
        resources.put(KubernetesConstants.CPU_RESOURCE, Quantity.fromString(Integer.toString(cpu)));
        return new V1ResourceRequirements().requests(resources).limits(new HashMap<>(resources));
    }

    private static V1Volume configMapVolume(String configMap) {
        return new V1Volume()
                .name(KubernetesConstants.CONFIG_VOLUME)
                .configMap(new V1ConfigMapVolumeSource().name(configMap));
    }

    private static List<String> command(
            KubernetesApplicationParameters parameters, int memory, String main, String... args) {
        String home = parameters.getSeatunnelHome();
        List<String> command = new ArrayList<>();
        command.add(KubernetesConstants.JAVA_COMMAND);
        command.add("-Xmx" + heapMb(memory) + "m");
        command.add("-XX:+ExitOnOutOfMemoryError");
        command.add("-Dhazelcast.logging.type=log4j2");
        command.add("-Dseatunnel.logs.path=" + home + "/logs");
        command.add("-Dseatunnel.logs.file_name=seatunnel-application");
        command.add("-Dseatunnel.home=" + home);
        command.add("-Dseatunnel.config=" + home + KubernetesConstants.SEATUNNEL_CONFIG_FILE);
        command.add("-Dlog4j2.configurationFile=" + home + KubernetesConstants.LOG4J_CONFIG_FILE);
        command.add("-cp");
        command.add(home + classpath(home));
        command.add(main);
        command.addAll(Arrays.asList(args));
        return command;
    }

    /**
     * Reserves 75% of the container memory for the JVM heap: {@code max(1 MiB, memory * 3 / 4)}.
     */
    private static long heapMb(int memoryMb) {
        return Math.max(
                KubernetesConstants.MINIMUM_JVM_HEAP_MB,
                memoryMb
                        * (long) KubernetesConstants.JVM_HEAP_NUMERATOR
                        / KubernetesConstants.JVM_HEAP_DENOMINATOR);
    }

    /** Builds the Java classpath shared by master and worker containers. */
    private static String classpath(String home) {
        return String.format(KubernetesConstants.KUBERNETES_CLASSPATH, home, home, home, home);
    }

    private static void addMasterProbes(
            V1Container container, int port, ApplicationSpecification specification) {
        V1Probe tcpProbe = tcpProbe(port);
        int startupFailures = startupFailureThreshold(specification.getStartupTimeoutMillis());
        container.setStartupProbe(
                tcpProbe(port)
                        .periodSeconds(KubernetesConstants.STARTUP_PROBE_PERIOD_MILLIS / 1000)
                        .failureThreshold(startupFailures));
        container.setReadinessProbe(tcpProbe);
        container.setLivenessProbe(tcpProbe(port));
    }

    /** Maps the startup timeout to probe failures: {@code max(1, ceil(timeout / probePeriod))}. */
    private static int startupFailureThreshold(long startupTimeoutMillis) {
        return Math.max(
                1,
                (int)
                        Math.ceil(
                                startupTimeoutMillis
                                        / (double)
                                                KubernetesConstants.STARTUP_PROBE_PERIOD_MILLIS));
    }

    private static V1Probe tcpProbe(int port) {
        return new V1Probe()
                .tcpSocket(new V1TCPSocketAction().port(new IntOrString(port)))
                .timeoutSeconds(KubernetesConstants.PROBE_TIMEOUT_SECONDS)
                .periodSeconds(KubernetesConstants.PROBE_PERIOD_SECONDS)
                .failureThreshold(KubernetesConstants.PROBE_FAILURE_THRESHOLD);
    }

    private static V1Probe processProbe() {
        V1ExecAction action =
                new V1ExecAction()
                        .command(
                                Arrays.asList(
                                        KubernetesConstants.SHELL_COMMAND,
                                        "-c",
                                        KubernetesConstants.PROCESS_PROBE_COMMAND));
        return new V1Probe()
                .exec(action)
                .timeoutSeconds(KubernetesConstants.PROBE_TIMEOUT_SECONDS)
                .periodSeconds(KubernetesConstants.PROBE_PERIOD_SECONDS)
                .failureThreshold(KubernetesConstants.PROBE_FAILURE_THRESHOLD);
    }
}
