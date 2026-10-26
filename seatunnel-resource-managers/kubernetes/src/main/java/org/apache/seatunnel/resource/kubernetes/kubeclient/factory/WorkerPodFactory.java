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
import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.worker.SeatunnelKubernetesApplicationWorker;

import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1Pod;

/**
 * Creates one worker Pod skeleton, owned by the application Job.
 *
 * <p>Workers are created by the application master, not by the submitter. The caller builds the
 * skeleton and then runs the shared Pod decorator chain over it, so worker and master Pods are
 * decorated by exactly the same steps.
 */
public final class WorkerPodFactory extends AbstractWorkloadFactory {

    private WorkerPodFactory(KubernetesApplicationConfig parameters) {
        super(parameters);
    }

    /** Builds the worker Pod skeleton: identity, owner reference, container and command. */
    public static V1Pod createWorkerPod(
            KubernetesJob owner,
            String name,
            KubernetesApplicationConfig parameters,
            WorkerSpecification resources,
            String clusterName,
            String masterAddress) {
        return new WorkerPodFactory(parameters)
                .workerPod(owner, name, resources, clusterName, masterAddress);
    }

    /**
     * Builds the worker Pod: the shared container and command. The Pod carries no probe; the master
     * detects a lost worker from the Kubernetes Pod watch instead.
     */
    private V1Pod workerPod(
            KubernetesJob owner,
            String name,
            WorkerSpecification resources,
            String clusterName,
            String masterAddress) {
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
                .spec(podSpec(container, false));
    }
}
