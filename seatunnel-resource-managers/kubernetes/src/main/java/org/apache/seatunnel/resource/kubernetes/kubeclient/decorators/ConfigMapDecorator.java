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
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesConfigMapEntry;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;

import io.kubernetes.client.openapi.models.V1PodSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * Mounts the configured {@code kubernetes.configmap} entries as read-only files.
 *
 * <p>Referenced ConfigMaps are never created or deleted. Entries that declare {@code data} are
 * created as ConfigMaps owned by the application Job, so cancelling the application removes them.
 * Keys are mounted through {@code subPath} at {@code <seatunnel-home>/config/<key>} unless a {@code
 * mount} entry overrides the path.
 */
public final class ConfigMapDecorator extends AbstractKubernetesStepDecorator {
    private final String applicationId;
    private final KubernetesApplicationParameters parameters;

    public ConfigMapDecorator(String applicationId, KubernetesApplicationParameters parameters) {
        this.applicationId = applicationId;
        this.parameters = parameters;
    }

    @Override
    public KubernetesJob decorateJob(KubernetesJob job) {
        V1PodSpec podSpec = job.getInternalResource().getSpec().getTemplate().getSpec();
        KubernetesResourceFactory.mountConfigMaps(podSpec, parameters, applicationId);
        return job;
    }

    @Override
    public List<KubernetesResource<?>> buildAccompanyingResources() {
        List<KubernetesResource<?>> configMaps = new ArrayList<>();
        for (KubernetesConfigMapEntry entry : parameters.getConfigMaps()) {
            if (!entry.isReference()) {
                configMaps.add(
                        KubernetesResourceFactory.configMap(
                                applicationId + "-" + entry.getEntryName(), entry.getData()));
            }
        }
        return configMaps;
    }
}
