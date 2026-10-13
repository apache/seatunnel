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

package org.apache.seatunnel.resource.kubernetes.kubeclient.decorators;

import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.worker.SeatunnelKubernetesApplicationWorker;

import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ExecAction;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Probe;

import java.util.Arrays;

/**
 * Creates one worker Pod resource and hands it to the rest of the worker chain.
 *
 * <p>Workers are created by the application master, not by the submitter. This step builds the Pod
 * skeleton, including its identity, owner reference, container, command and probes; every later
 * step adds the features shared with the master, so worker and master Pods cannot drift apart.
 */
public final class PodDecorator extends AbstractWorkloadDecorator {
    private final KubernetesJob owner;
    private final String name;
    private final WorkerSpecification resources;
    private final String clusterName;
    private final String masterAddress;

    public PodDecorator(
            KubernetesJob owner,
            String name,
            KubernetesApplicationParameters parameters,
            WorkerSpecification resources,
            String clusterName,
            String masterAddress) {
        super(parameters);
        this.owner = owner;
        this.name = name;
        this.resources = resources;
        this.clusterName = clusterName;
        this.masterAddress = masterAddress;
    }

    @Override
    public V1Pod decoratePod(V1Pod pod) {
        if (pod != null) {
            throw new IllegalStateException("The Pod step must start the worker decorator chain");
        }
        return new V1Pod()
                .apiVersion(KubernetesConstants.CORE_API_VERSION)
                .kind(KubernetesConstants.POD_KIND)
                .metadata(
                        KubernetesResourceIdentity.podMetadata(
                                owner,
                                name,
                                KubernetesConstants.WORKER_ROLE,
                                parameters.getWorkerLabels(),
                                parameters.getWorkerAnnotations()))
                .spec(workerPod());
    }

    /**
     * Builds the worker Pod: the shared container and command, joined by the process probe that
     * detects a dead worker without restarting it.
     */
    private V1PodSpec workerPod() {
        int memoryMb = resources.getMemoryMb();
        V1Container container = container(memoryMb, resources.getCpuCores());
        container.setCommand(
                command(
                        memoryMb,
                        parameters.getWorkerHazelcastConfig(),
                        parameters.getWorkerJvmOptionsFiles(),
                        SeatunnelKubernetesApplicationWorker.class.getName(),
                        clusterName,
                        masterAddress,
                        Integer.toString(resources.getSlots())));
        container.setLivenessProbe(processProbe());
        return podSpec(container, false);
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
