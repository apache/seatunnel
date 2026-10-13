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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds the complete application composition on the client side.
 *
 * <p>Decorators are applied in order: each one may decorate the owner Job and contribute
 * accompanying resources. This factory makes no API calls.
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
        for (KubernetesStepDecorator decorator : decorators(applicationId, parameters)) {
            job = decorator.decorateJob(job);
            accompanyingResources.addAll(decorator.buildAccompanyingResources());
        }
        return new KubernetesApplicationSpecification(job, accompanyingResources);
    }

    private static List<KubernetesStepDecorator> decorators(
            String applicationId, KubernetesApplicationParameters parameters) {
        return Arrays.asList(
                new InlineSpecificationDecorator(parameters),
                new EnvironmentDecorator(parameters),
                new SecretDecorator(applicationId, parameters),
                new ConfigMapDecorator(applicationId, parameters),
                new HeadlessServiceDecorator(applicationId, parameters));
    }
}
