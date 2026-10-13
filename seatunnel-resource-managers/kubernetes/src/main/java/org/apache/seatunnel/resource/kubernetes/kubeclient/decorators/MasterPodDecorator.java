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

import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;

import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * Runs the Pod decorator chain over the master template of the owner Job.
 *
 * <p>The Job step creates the master template; this step is the single place where the shared Pod
 * chain is applied to it, so the master Pod and the worker Pods are decorated by exactly the same
 * steps.
 */
public final class MasterPodDecorator extends AbstractKubernetesStepDecorator<KubernetesJob> {
    private final List<KubernetesStepDecorator<V1Pod>> steps;

    public MasterPodDecorator(List<KubernetesStepDecorator<V1Pod>> steps) {
        this.steps = steps;
    }

    @Override
    public KubernetesJob decorate(KubernetesJob job) {
        if (job == null) {
            throw new IllegalStateException(
                    "The master Pod step must follow the step that creates the application Job");
        }
        V1PodTemplateSpec template = job.getInternalResource().getSpec().getTemplate();
        V1Pod pod = new V1Pod().metadata(template.getMetadata()).spec(template.getSpec());
        for (KubernetesStepDecorator<V1Pod> step : steps) {
            pod = step.decorate(pod);
        }
        template.setMetadata(pod.getMetadata());
        template.setSpec(pod.getSpec());
        return job;
    }

    @Override
    public List<KubernetesResource<?>> build() {
        List<KubernetesResource<?>> resources = new ArrayList<>();
        for (KubernetesStepDecorator<V1Pod> step : steps) {
            resources.addAll(step.build());
        }
        return resources;
    }
}
