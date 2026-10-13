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
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;
import org.apache.seatunnel.resource.kubernetes.kubeclient.services.HeadlessClusterIPService;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import java.util.Collections;
import java.util.List;

/**
 * Adds the headless Service used for master discovery to the application resources.
 *
 * <p>The Service selects exactly the master Pod of this application, and publishes not-ready
 * addresses so workers can join while the master is still starting.
 */
public final class HeadlessServiceDecorator extends AbstractKubernetesStepDecorator {
    private final String applicationId;
    private final KubernetesApplicationParameters parameters;

    public HeadlessServiceDecorator(
            String applicationId, KubernetesApplicationParameters parameters) {
        this.applicationId = applicationId;
        this.parameters = parameters;
    }

    @Override
    public List<KubernetesResource<?>> buildAccompanyingResources() {
        V1ObjectMeta metadata =
                KubernetesResourceFactory.metadata(applicationId, KubernetesConstants.MASTER_ROLE);
        return Collections.singletonList(
                new HeadlessClusterIPService(
                        metadata,
                        metadata.getLabels(),
                        KubernetesConstants.HAZELCAST_PORT_NAME,
                        parameters.getSpecification().getMasterPort()));
    }
}
