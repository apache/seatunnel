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

package org.apache.seatunnel.resource.kubernetes.kubeclient.services;

import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesService;

import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;

import java.util.Map;

/** ClusterIP Service exposing one TCP endpoint selected by Pod labels. */
public class ClusterIPService extends KubernetesService {
    private static final String CORE_API_VERSION = "v1";
    private static final String SERVICE_KIND = "Service";

    /**
     * Builds a regular ClusterIP service whose virtual IP is assigned by Kubernetes.
     *
     * @param metadata service identity and owner reference
     * @param selector labels selecting backend Pods
     * @param portName stable endpoint name
     * @param port exposed and target TCP port
     */
    public ClusterIPService(
            V1ObjectMeta metadata, Map<String, String> selector, String portName, int port) {
        this(metadata, selector, portName, port, null, false);
    }

    protected ClusterIPService(
            V1ObjectMeta metadata,
            Map<String, String> selector,
            String portName,
            int port,
            String clusterIp,
            boolean publishNotReadyAddresses) {
        super(
                new V1Service()
                        .apiVersion(CORE_API_VERSION)
                        .kind(SERVICE_KIND)
                        .metadata(metadata)
                        .spec(
                                new V1ServiceSpec()
                                        .clusterIP(clusterIp)
                                        .publishNotReadyAddresses(publishNotReadyAddresses)
                                        .selector(selector)
                                        .addPortsItem(
                                                new V1ServicePort()
                                                        .name(portName)
                                                        .port(port)
                                                        .targetPort(new IntOrString(port)))));
    }
}
