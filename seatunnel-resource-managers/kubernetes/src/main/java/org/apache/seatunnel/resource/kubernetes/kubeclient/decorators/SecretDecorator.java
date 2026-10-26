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
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesSecret;

import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1SecretVolumeSource;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Mounts the configured application Secret read-only at {@code kubernetes.secret-directory} in
 * every application Pod.
 *
 * <p>{@code kubernetes.secret-ref} only references a Secret managed outside SeaTunnel. Otherwise
 * the decorator creates the Secret from {@code kubernetes.secret.data} entries; a {@code job.conf}
 * entry then replaces the inline job configuration on the master. Without either option no Secret
 * is created, mounted or deleted.
 */
public final class SecretDecorator extends AbstractKubernetesStepDecorator {
    private final String applicationId;
    private final KubernetesApplicationConfig parameters;

    public SecretDecorator(String applicationId, KubernetesApplicationConfig parameters) {
        this.applicationId = applicationId;
        this.parameters = parameters;
    }

    @Override
    public V1Pod decorate(V1Pod pod) {
        if (!parameters.hasApplicationSecret()) {
            return pod;
        }
        mount(pod.getSpec(), parameters.resolvedApplicationSecretName(applicationId));
        return pod;
    }

    @Override
    public List<KubernetesResource<?>> build() {
        if (parameters.getApplicationSecretData().isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.singletonList(
                new KubernetesSecret(
                        new V1Secret()
                                .apiVersion(KubernetesConstants.CORE_API_VERSION)
                                .kind(KubernetesConstants.SECRET_KIND)
                                .type(KubernetesConstants.OPAQUE_SECRET_TYPE)
                                .metadata(
                                        KubernetesResourceIdentity.metadata(
                                                applicationId,
                                                parameters.resolvedApplicationSecretName(
                                                        applicationId),
                                                KubernetesConstants.CONFIGURATION_ROLE))
                                .stringData(
                                        new LinkedHashMap<>(
                                                parameters.getApplicationSecretData()))));
    }

    /**
     * Mounts the application Secret read-only into one Pod specification.
     *
     * @param podSpec Pod specification to decorate
     * @param secretName referenced Secret name or the name of the application-owned one
     */
    private void mount(V1PodSpec podSpec, String secretName) {
        podSpec.addVolumesItem(
                new V1Volume()
                        .name(KubernetesConstants.APPLICATION_VOLUME)
                        .secret(
                                new V1SecretVolumeSource()
                                        .secretName(secretName)
                                        .defaultMode(KubernetesConstants.APPLICATION_SECRET_MODE)));
        podSpec.getContainers()
                .get(0)
                .addVolumeMountsItem(
                        new V1VolumeMount()
                                .name(KubernetesConstants.APPLICATION_VOLUME)
                                .mountPath(parameters.getSecretDirectory())
                                .readOnly(true));
    }
}
