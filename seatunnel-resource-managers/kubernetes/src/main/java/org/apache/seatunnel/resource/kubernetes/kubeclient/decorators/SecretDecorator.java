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

import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;

import io.kubernetes.client.openapi.models.V1PodSpec;

import java.util.Collections;
import java.util.List;

/**
 * Mounts the configured application Secret read-only at {@link
 * KubernetesConstants#APPLICATION_SECRET_DIRECTORY}.
 *
 * <p>{@code kubernetes.secret-ref} only references a Secret managed outside SeaTunnel. Otherwise
 * the decorator creates the Secret from {@code kubernetes.secret.data} entries; a {@code job.conf}
 * entry then replaces the inline job configuration on the master. Without either option no Secret
 * is created, mounted or deleted.
 */
public final class SecretDecorator extends AbstractKubernetesStepDecorator {
    private final String applicationId;
    private final KubernetesApplicationParameters parameters;

    public SecretDecorator(String applicationId, KubernetesApplicationParameters parameters) {
        this.applicationId = applicationId;
        this.parameters = parameters;
    }

    @Override
    public KubernetesJob decorateJob(KubernetesJob job) {
        if (!parameters.hasApplicationSecret()) {
            return job;
        }
        V1PodSpec podSpec = job.getInternalResource().getSpec().getTemplate().getSpec();
        KubernetesResourceFactory.mountApplicationSecret(
                podSpec, parameters.resolvedApplicationSecretName(applicationId));
        return job;
    }

    @Override
    public List<KubernetesResource<?>> buildAccompanyingResources() {
        if (parameters.getApplicationSecretData().isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.singletonList(
                KubernetesResourceFactory.secret(
                        parameters.resolvedApplicationSecretName(applicationId),
                        parameters.getApplicationSecretData()));
    }
}
