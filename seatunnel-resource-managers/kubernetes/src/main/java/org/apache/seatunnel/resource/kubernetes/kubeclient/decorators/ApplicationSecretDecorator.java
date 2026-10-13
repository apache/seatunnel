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

import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1SecretVolumeSource;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Mounts the credential-bearing application specification Secret on the master container.
 *
 * <p>The Secret is created after the owner Job so that it can carry the Job's owner reference.
 */
public final class ApplicationSecretDecorator extends AbstractKubernetesStepDecorator {
    private final String applicationId;
    private final KubernetesApplicationParameters parameters;

    public ApplicationSecretDecorator(
            String applicationId, KubernetesApplicationParameters parameters) {
        this.applicationId = applicationId;
        this.parameters = parameters;
    }

    @Override
    public KubernetesJob decorateJob(KubernetesJob job) {
        V1PodSpec podSpec = job.getInternalResource().getSpec().getTemplate().getSpec();
        podSpec.addVolumesItem(applicationVolume());
        V1Container master = podSpec.getContainers().get(0);
        master.addVolumeMountsItem(
                new V1VolumeMount()
                        .name(KubernetesConstants.APPLICATION_VOLUME)
                        .mountPath(KubernetesConstants.CONFIG_DIRECTORY)
                        .readOnly(true));
        return job;
    }

    @Override
    public List<KubernetesResource<?>> buildAccompanyingResources() throws IOException {
        return Collections.singletonList(
                KubernetesResourceFactory.secret(applicationId, parameters));
    }

    private V1Volume applicationVolume() {
        return new V1Volume()
                .name(KubernetesConstants.APPLICATION_VOLUME)
                .secret(
                        new V1SecretVolumeSource()
                                .secretName(applicationId)
                                .defaultMode(KubernetesConstants.APPLICATION_SECRET_MODE));
    }
}
