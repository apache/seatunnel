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
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesApplicationSpecification;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.ConfigMapDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.EnvironmentDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.HeadlessServiceDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.InlineSpecificationDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.JobDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.KubernetesStepDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.PodDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.SecretDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;

import io.kubernetes.client.openapi.models.V1Pod;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Runs the application decorator chains and returns the composed Kubernetes resources.
 *
 * <p>Every resource is created by a chain step: the first step of the master chain creates the
 * owner Job, the first step of the worker chain creates the worker Pod, and the remaining steps
 * decorate them or contribute accompanying resources. Role differences live here, in the
 * composition: the master chain adds the features only the master has, while the worker chain
 * reuses the shared Pod features. No decorator branches on its role. This factory makes no API
 * calls.
 */
public final class KubernetesApplicationFactory {

    private KubernetesApplicationFactory() {}

    /**
     * Builds the suspended owner Job and every resource that must exist next to it.
     *
     * @param applicationId generated Kubernetes application Job name
     * @param mainClass master entrypoint class
     * @param parameters resolved application and Kubernetes settings
     * @return application composition ready for creation
     */
    public static KubernetesApplicationSpecification buildApplicationSpecification(
            String applicationId, String mainClass, KubernetesApplicationParameters parameters) {
        KubernetesJob job = null;
        List<KubernetesResource<?>> accompanyingResources = new ArrayList<>();
        for (KubernetesStepDecorator decorator :
                masterDecorators(applicationId, mainClass, parameters)) {
            job = decorator.decorateJob(job);
            accompanyingResources.addAll(decorator.buildAccompanyingResources());
        }
        return new KubernetesApplicationSpecification(job, accompanyingResources);
    }

    /**
     * Runs the worker decorator chain for one worker of this application.
     *
     * <p>The first step creates the Pod and the chain then applies exactly the features shared with
     * the master, so worker Pods cannot drift away from the master's mounts, environment and
     * runtime configuration. Workers are created by the application master through the resource
     * manager, so this method runs in-cluster with the same parameters the master restored from the
     * localized specification.
     *
     * @param owner owner Job whose UID backs the Pod's owner reference
     * @param name generated worker Pod name
     * @param parameters resolved application and Kubernetes settings
     * @param resources per-worker resource and slot settings
     * @param clusterName isolated Hazelcast cluster name
     * @param masterAddress advertised application master address
     * @return worker Pod ready to create
     */
    public static KubernetesPod buildWorkerPod(
            KubernetesJob owner,
            String name,
            KubernetesApplicationParameters parameters,
            WorkerSpecification resources,
            String clusterName,
            String masterAddress) {
        V1Pod pod = null;
        for (KubernetesStepDecorator decorator :
                workerDecorators(owner, name, parameters, resources, clusterName, masterAddress)) {
            pod = decorator.decoratePod(pod);
        }
        return new KubernetesPod(pod);
    }

    /**
     * Composes the chain applied to the owner Job's master Pod and to the accompanying resources.
     */
    private static List<KubernetesStepDecorator> masterDecorators(
            String applicationId, String mainClass, KubernetesApplicationParameters parameters) {
        List<KubernetesStepDecorator> decorators = new ArrayList<>();
        decorators.add(new JobDecorator(applicationId, mainClass, parameters));
        decorators.add(new InlineSpecificationDecorator(parameters));
        decorators.addAll(sharedPodDecorators(applicationId, parameters));
        decorators.add(new HeadlessServiceDecorator(applicationId, parameters));
        return decorators;
    }

    /** Composes the chain applied to every worker Pod created by the master. */
    private static List<KubernetesStepDecorator> workerDecorators(
            KubernetesJob owner,
            String name,
            KubernetesApplicationParameters parameters,
            WorkerSpecification resources,
            String clusterName,
            String masterAddress) {
        List<KubernetesStepDecorator> decorators = new ArrayList<>();
        decorators.add(
                new PodDecorator(owner, name, parameters, resources, clusterName, masterAddress));
        decorators.addAll(sharedPodDecorators(owner.getName(), parameters));
        return decorators;
    }

    /** Pod features that the master and every worker need to run the same job. */
    private static List<KubernetesStepDecorator> sharedPodDecorators(
            String applicationId, KubernetesApplicationParameters parameters) {
        return Arrays.asList(
                new EnvironmentDecorator(parameters),
                new SecretDecorator(applicationId, parameters),
                new ConfigMapDecorator(applicationId, parameters));
    }
}
