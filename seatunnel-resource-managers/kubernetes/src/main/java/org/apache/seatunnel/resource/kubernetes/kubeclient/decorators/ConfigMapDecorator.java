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

import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesConfigMapEntry;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapVolumeSource;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

/**
 * Mounts the configured {@code kubernetes.configmap} entries as read-only files in every
 * application Pod.
 *
 * <p>Referenced ConfigMaps are never created or deleted. Entries that declare {@code data} are
 * created as ConfigMaps owned by the application Job, so cancelling the application removes them.
 * Every mounted key declares its full container path through {@code mount} and is mounted through
 * {@code subPath}, so entries never overwrite each other.
 */
public final class ConfigMapDecorator extends AbstractKubernetesStepDecorator {
    private final String applicationId;
    private final KubernetesApplicationConfig parameters;

    public ConfigMapDecorator(String applicationId, KubernetesApplicationConfig parameters) {
        this.applicationId = applicationId;
        this.parameters = parameters;
    }

    @Override
    public V1Pod decorate(V1Pod pod) {
        V1PodSpec podSpec = pod.getSpec();
        int index = 0;
        for (KubernetesConfigMapEntry entry : parameters.getConfigMaps()) {
            String volumeName = KubernetesConstants.CONFIG_VOLUME + "-" + index++;
            podSpec.addVolumesItem(
                    new V1Volume()
                            .name(volumeName)
                            .configMap(new V1ConfigMapVolumeSource().name(resolvedName(entry))));
            for (String key : mountedKeys(entry)) {
                podSpec.getContainers()
                        .get(0)
                        .addVolumeMountsItem(
                                new V1VolumeMount()
                                        .name(volumeName)
                                        .mountPath(entry.mountPath(key))
                                        .subPath(key)
                                        .readOnly(true));
            }
        }
        return pod;
    }

    @Override
    public List<KubernetesResource<?>> build() {
        List<KubernetesResource<?>> configMaps = new ArrayList<>();
        for (KubernetesConfigMapEntry entry : parameters.getConfigMaps()) {
            if (!entry.isReference()) {
                configMaps.add(
                        new KubernetesConfigMap(
                                new V1ConfigMap()
                                        .apiVersion(KubernetesConstants.CORE_API_VERSION)
                                        .kind(KubernetesConstants.CONFIG_MAP_KIND)
                                        .metadata(
                                                KubernetesResourceIdentity.metadata(
                                                        applicationId,
                                                        resolvedName(entry),
                                                        KubernetesConstants.CONFIGURATION_ROLE))
                                        .data(new LinkedHashMap<>(entry.getData()))));
            }
        }
        return configMaps;
    }

    /**
     * Resolves the ConfigMap name, and mounts every declared key through {@code subPath} so entries
     * never overwrite each other or the image's own configuration files.
     */
    private String resolvedName(KubernetesConfigMapEntry entry) {
        return entry.isReference() ? entry.getReference() : entry.createdName(applicationId);
    }

    private static Set<String> mountedKeys(KubernetesConfigMapEntry entry) {
        return entry.isReference() ? entry.getMountPaths().keySet() : entry.getData().keySet();
    }
}
