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

import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesEnvironmentVariable;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;

import io.kubernetes.client.openapi.models.V1Container;

/**
 * Injects the configured {@code kubernetes.env} entries into the master container.
 *
 * <p>Worker containers receive the same entries when the master creates them. Kubernetes resolves
 * Secret and ConfigMap references, so placeholders in the job configuration are resolved inside the
 * Pod even when the submitting host cannot see those values.
 */
public final class EnvironmentDecorator extends AbstractKubernetesStepDecorator {
    private final KubernetesApplicationParameters parameters;

    public EnvironmentDecorator(KubernetesApplicationParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public KubernetesJob decorateJob(KubernetesJob job) {
        V1Container master =
                job.getInternalResource().getSpec().getTemplate().getSpec().getContainers().get(0);
        for (KubernetesEnvironmentVariable variable : parameters.getEnvironmentVariables()) {
            master.addEnvItem(KubernetesResourceFactory.environmentVariable(variable));
        }
        return job;
    }
}
