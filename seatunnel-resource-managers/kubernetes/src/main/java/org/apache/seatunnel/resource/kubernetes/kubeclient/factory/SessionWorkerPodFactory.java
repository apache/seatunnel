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

import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesSessionConfig;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.ConfigMapDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.DataVolumeDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.EnvironmentDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.KubernetesStepDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.SecretDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;

import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1Pod;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates one bare session worker Pod owned by the session root ConfigMap.
 *
 * <p>The Pod skeleton, resources and shared Pod-spec fields come from {@link
 * AbstractWorkloadFactory}, the same base used by application workers. Only the owner, entrypoint
 * and session-specific label differ. Environment, Secret and ConfigMap features are applied through
 * the shared decorator chain.
 */
public final class SessionWorkerPodFactory extends AbstractWorkloadFactory {

    private final KubernetesSessionConfig parameters;

    private SessionWorkerPodFactory(KubernetesSessionConfig parameters) {
        super(parameters);
        this.parameters = parameters;
    }

    /**
     * Builds one session worker Pod.
     *
     * @param root session root ConfigMap supplying the owner reference
     * @param name generated Pod name
     * @param config session and Kubernetes template settings
     * @param resources worker resource envelope
     * @return Pod ready for {@code createNamespacedPod}
     */
    public static V1Pod createWorkerPod(
            KubernetesConfigMap root,
            String name,
            KubernetesSessionConfig config,
            WorkerSpecification resources) {
        return new SessionWorkerPodFactory(config).workerPod(root, name, resources);
    }

    private V1Pod workerPod(KubernetesConfigMap root, String name, WorkerSpecification resources) {
        String sessionName = parameters.getSessionSpecification().getName();
        Map<String, String> labels = new LinkedHashMap<>(parameters.getWorkerLabels());
        labels.put(KubernetesConstants.SESSION_LABEL, sessionName);

        V1Container container = container(resources.getMemoryMb(), resources.getCpuCores());
        container.setCommand(workerCommand());

        V1Pod pod =
                new V1Pod()
                        .apiVersion(KubernetesConstants.CORE_API_VERSION)
                        .kind(KubernetesConstants.POD_KIND)
                        .metadata(
                                KubernetesResourceIdentity.podMetadata(
                                                sessionName,
                                                KubernetesConstants.WORKER_ROLE,
                                                labels,
                                                parameters.getWorkerAnnotations())
                                        .name(name)
                                        .ownerReferences(
                                                Collections.singletonList(ownerReference(root))))
                        .spec(podSpec(container, false));

        List<KubernetesStepDecorator> decorators = new ArrayList<>();
        decorators.add(
                new DataVolumeDecorator(parameters.getDataClaim(), parameters.getDataMount()));
        decorators.add(new EnvironmentDecorator(parameters));
        decorators.add(new SecretDecorator(sessionName, parameters));
        decorators.add(new ConfigMapDecorator(sessionName, parameters.getWorkerConfigMapEntries()));
        for (KubernetesStepDecorator decorator : decorators) {
            pod = decorator.decorate(pod);
        }
        return pod;
    }

    private List<String> workerCommand() {
        return Arrays.asList(
                parameters.getSeatunnelHome() + "/" + KubernetesConstants.SESSION_ENTRYPOINT,
                KubernetesConstants.NODE_ROLE_FLAG,
                KubernetesConstants.WORKER_ROLE,
                KubernetesConstants.JVM_OPTION_PREFIX + parameters.getJvmOptions());
    }

    private static V1OwnerReference ownerReference(KubernetesConfigMap root) {
        if (root.getInternalResource().getMetadata() == null
                || root.getInternalResource().getMetadata().getUid() == null) {
            throw new IllegalArgumentException(
                    "Session root ConfigMap " + root.getName() + " has no UID");
        }
        return new V1OwnerReference()
                .apiVersion(KubernetesConstants.CORE_API_VERSION)
                .kind(KubernetesConstants.CONFIG_MAP_KIND)
                .name(root.getName())
                .uid(root.getInternalResource().getMetadata().getUid())
                .controller(false)
                .blockOwnerDeletion(false);
    }
}
