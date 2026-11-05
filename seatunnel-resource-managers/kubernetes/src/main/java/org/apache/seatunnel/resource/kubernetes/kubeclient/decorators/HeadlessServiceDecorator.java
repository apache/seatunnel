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
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesService;

import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;

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
    private final KubernetesApplicationConfig parameters;

    public HeadlessServiceDecorator(String applicationId, KubernetesApplicationConfig parameters) {
        this.applicationId = applicationId;
        this.parameters = parameters;
    }

    @Override
    public List<KubernetesResource<?>> build() {
        V1ObjectMeta metadata =
                KubernetesResourceIdentity.metadata(
                        applicationId, applicationId, KubernetesConstants.MASTER_ROLE);
        int port = parameters.getSpecification().getMasterPort();
        return Collections.singletonList(
                new KubernetesService(
                        new V1Service()
                                .apiVersion(KubernetesConstants.CORE_API_VERSION)
                                .kind(KubernetesConstants.SERVICE_KIND)
                                .metadata(metadata)
                                .spec(
                                        new V1ServiceSpec()
                                                .clusterIP(KubernetesConstants.HEADLESS_CLUSTER_IP)
                                                .publishNotReadyAddresses(true)
                                                .selector(metadata.getLabels())
                                                .addPortsItem(
                                                        new V1ServicePort()
                                                                .name(
                                                                        KubernetesConstants
                                                                                .HAZELCAST_PORT_NAME)
                                                                .port(port)
                                                                .targetPort(
                                                                        new IntOrString(port))))));
    }
}
