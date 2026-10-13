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

import java.util.Collections;
import java.util.List;

/** No-op defaults and the master Pod routing shared by step decorators. */
public abstract class AbstractKubernetesStepDecorator implements KubernetesStepDecorator {

    /**
     * Routes the owner Job's master Pod through {@link #decoratePod(V1Pod)}.
     *
     * @param job owner Job being built
     * @return the Job whose master template contains this feature's changes
     */
    @Override
    public KubernetesJob decorateJob(KubernetesJob job) {
        if (job == null) {
            throw new IllegalStateException(
                    getClass().getSimpleName()
                            + " must follow the step that creates the application Job");
        }
        V1PodTemplateSpec template = job.getInternalResource().getSpec().getTemplate();
        V1Pod decorated =
                decoratePod(new V1Pod().metadata(template.getMetadata()).spec(template.getSpec()));
        template.setMetadata(decorated.getMetadata());
        template.setSpec(decorated.getSpec());
        return job;
    }

    @Override
    public V1Pod decoratePod(V1Pod pod) {
        if (pod == null) {
            throw new IllegalStateException(
                    getClass().getSimpleName() + " must follow the step that creates the Pod");
        }
        return pod;
    }

    @Override
    public List<KubernetesResource<?>> buildAccompanyingResources() {
        return Collections.emptyList();
    }
}
