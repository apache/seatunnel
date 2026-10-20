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

import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesToleration;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1LocalObjectReference;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Toleration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Shared assembly for the factories that create the application's workload Pod skeletons.
 *
 * <p>{@link ApplicationJobFactory} creates the master Pod that is later wrapped into the owner Job
 * and {@link WorkerPodFactory} creates the worker Pods; both extend this class so the container
 * skeleton, the JVM command, the resource requirements and the Pod spec stay identical across
 * roles. Role-specific parts stay in the owning factory: the master probes and the checkpoint
 * claim.
 */
public abstract class AbstractWorkloadFactory {

    protected final KubernetesApplicationConfig parameters;

    protected AbstractWorkloadFactory(KubernetesApplicationConfig parameters) {
        this.parameters = parameters;
    }

    /** Builds the application container with its image, resources and environment. */
    protected V1Container container(int memoryMb, int cpuCores) {
        String home = parameters.getSeatunnelHome();
        return new V1Container()
                .name(KubernetesConstants.CONTAINER_NAME)
                .image(parameters.getImage())
                .imagePullPolicy(parameters.getImagePullPolicy())
                .workingDir(home)
                .addEnvItem(new V1EnvVar().name(KubernetesConstants.SEATUNNEL_HOME_ENV).value(home))
                .resources(resourceRequirements(memoryMb, cpuCores));
    }

    /** Builds the Pod skeleton shared by the master template and worker Pods. */
    protected V1PodSpec podSpec(V1Container container, boolean master) {
        V1PodSpec pod =
                new V1PodSpec()
                        .restartPolicy(KubernetesConstants.RESTART_POLICY_NEVER)
                        .terminationGracePeriodSeconds(
                                KubernetesConstants.TERMINATION_GRACE_PERIOD_SECONDS)
                        .serviceAccountName(parameters.getServiceAccount())
                        .automountServiceAccountToken(master)
                        .containers(Collections.singletonList(container));
        for (String secret : parameters.getImagePullSecrets()) {
            pod.addImagePullSecretsItem(new V1LocalObjectReference().name(secret));
        }
        pod.setNodeSelector(
                master ? parameters.getMasterNodeSelector() : parameters.getWorkerNodeSelector());
        addTolerations(pod, master);
        return pod;
    }

    /** Applies the role's tolerations so tainted node pools stay schedulable. */
    private void addTolerations(V1PodSpec pod, boolean master) {
        List<KubernetesToleration> tolerations =
                master ? parameters.getMasterTolerations() : parameters.getWorkerTolerations();
        if (tolerations.isEmpty()) {
            return;
        }
        pod.setTolerations(
                tolerations.stream()
                        .map(AbstractWorkloadFactory::kubernetesToleration)
                        .collect(Collectors.toList()));
    }

    private static V1Toleration kubernetesToleration(KubernetesToleration toleration) {
        V1Toleration result =
                new V1Toleration().key(toleration.getKey()).operator(toleration.getOperator());
        if (!toleration.getValue().isEmpty()) {
            result.value(toleration.getValue());
        }
        if (!toleration.getEffect().isEmpty()) {
            result.effect(toleration.getEffect());
        }
        return result;
    }

    private static V1ResourceRequirements resourceRequirements(int memoryMb, int cpuCores) {
        Map<String, Quantity> resources = new HashMap<>();
        resources.put(
                KubernetesConstants.MEMORY_RESOURCE,
                Quantity.fromString(memoryMb + KubernetesConstants.MEBIBYTE_SUFFIX));
        resources.put(
                KubernetesConstants.CPU_RESOURCE, Quantity.fromString(Integer.toString(cpuCores)));
        return new V1ResourceRequirements().requests(resources).limits(new HashMap<>(resources));
    }

    /**
     * Builds the JVM command shared by master and worker containers.
     *
     * @param memoryMb container memory limit used for the heap size
     * @param hazelcastConfig role-specific Hazelcast configuration file, or {@code null}
     * @param jvmOptionsFiles ordered argument files applied through {@code @argfile}
     * @param main entrypoint class
     * @param args entrypoint arguments
     * @return the full container command
     */
    protected List<String> command(
            int memoryMb,
            String hazelcastConfig,
            List<String> jvmOptionsFiles,
            String main,
            String... args) {
        String home = parameters.getSeatunnelHome();
        List<String> command = new ArrayList<>();
        command.add(KubernetesConstants.JAVA_COMMAND);
        command.add(KubernetesConstants.JVM_MAX_HEAP_PREFIX + heapMb(memoryMb) + "m");
        command.add(KubernetesConstants.JVM_EXIT_ON_OUT_OF_MEMORY);
        command.addAll(KubernetesConstants.MANDATORY_JVM_MODULE_FLAGS);
        command.add(KubernetesConstants.SYSPROP_HAZELCAST_LOGGING);
        command.add(KubernetesConstants.SYSPROP_LOGS_PATH + home + "/logs");
        command.add(KubernetesConstants.SYSPROP_LOGS_FILE_NAME + "seatunnel-application");
        command.add(KubernetesConstants.SYSPROP_SEATUNNEL_HOME + home);
        if (parameters.getSeatunnelConfigFile() != null) {
            command.add(
                    KubernetesConstants.SYSPROP_SEATUNNEL_CONFIG
                            + parameters.getSeatunnelConfigFile());
        }
        if (parameters.getLog4jConfigFile() != null) {
            command.add(KubernetesConstants.SYSPROP_LOG4J_CONFIG + parameters.getLog4jConfigFile());
        }
        if (hazelcastConfig != null) {
            command.add(KubernetesConstants.SYSPROP_HAZELCAST_CONFIG + hazelcastConfig);
        }
        for (String jvmOptionsFile : jvmOptionsFiles) {
            command.add("@" + jvmOptionsFile);
        }
        command.add("-cp");
        command.add(classpath(home));
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
}
