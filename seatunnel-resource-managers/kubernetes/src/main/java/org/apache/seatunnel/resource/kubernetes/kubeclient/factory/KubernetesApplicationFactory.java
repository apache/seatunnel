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

import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesApplicationSpecification;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.ConfigMapDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.EnvironmentDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.HeadlessServiceDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.InlineSpecificationDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.KubernetesStepDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.SecretDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;

import io.kubernetes.client.openapi.models.V1Pod;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds the complete application composition on the client side.
 *
 * <p>Decorators are applied in order: each one may decorate Pod specifications and contribute
 * accompanying resources. Role differences live here, in the composition: the master chain adds the
 * features only the master has, while the worker chain reuses the shared Pod features. No decorator
 * branches on its role. This factory makes no API calls.
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
     * @throws IOException if the localized specification cannot be serialized
     */
    public static KubernetesApplicationSpecification buildApplicationSpecification(
            String applicationId, String mainClass, KubernetesApplicationParameters parameters)
            throws IOException {
        KubernetesJob job = KubernetesResourceFactory.job(applicationId, mainClass, parameters);
        List<KubernetesResource<?>> accompanyingResources = new ArrayList<>();
        for (KubernetesStepDecorator decorator : masterDecorators(applicationId, parameters)) {
            job = decorator.decorateJob(job);
            accompanyingResources.addAll(decorator.buildAccompanyingResources());
        }
        return new KubernetesApplicationSpecification(job, accompanyingResources);
    }

    /**
     * Applies the worker decorator chain to one worker Pod.
     *
     * <p>The chain contains exactly the features shared with the master, so worker Pods cannot
     * drift away from the master's mounts, environment and runtime configuration. Workers are
     * created by the application master through the resource manager, so this method runs
     * in-cluster with the same parameters the master restored from the localized specification.
     *
     * @param pod worker Pod being assembled
     * @param applicationId generated application Job name that owns the worker
     * @param parameters resolved application and Kubernetes settings
     * @return the decorated Pod
     */
    public static V1Pod decorateWorker(
            V1Pod pod, String applicationId, KubernetesApplicationParameters parameters) {
        V1Pod decorated = pod;
        for (KubernetesStepDecorator decorator : workerDecorators(applicationId, parameters)) {
            decorated = decorator.decoratePod(decorated);
        }
        return decorated;
    }

    /**
     * Composes the chain applied to the owner Job's master Pod and to the accompanying resources.
     */
    private static List<KubernetesStepDecorator> masterDecorators(
            String applicationId, KubernetesApplicationParameters parameters) {
        List<KubernetesStepDecorator> decorators = new ArrayList<>();
        decorators.add(new InlineSpecificationDecorator(parameters));
        decorators.addAll(sharedPodDecorators(applicationId, parameters));
        decorators.add(new HeadlessServiceDecorator(applicationId, parameters));
        return decorators;
    }

    /** Composes the chain applied to every worker Pod created by the master. */
    private static List<KubernetesStepDecorator> workerDecorators(
            String applicationId, KubernetesApplicationParameters parameters) {
        return sharedPodDecorators(applicationId, parameters);
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
