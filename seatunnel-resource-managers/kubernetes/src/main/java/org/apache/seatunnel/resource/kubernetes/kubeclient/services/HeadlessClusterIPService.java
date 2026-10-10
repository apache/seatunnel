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

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import java.util.Map;

/** Headless ClusterIP Service that publishes the master address before Pod readiness. */
public final class HeadlessClusterIPService extends ClusterIPService {
    private static final String HEADLESS_CLUSTER_IP = "None";

    /**
     * Builds a headless service for a single named TCP endpoint.
     *
     * @param metadata service identity and owner reference
     * @param selector labels selecting the application master Pod
     * @param portName stable endpoint name
     * @param port exposed and target TCP port
     */
    public HeadlessClusterIPService(
            V1ObjectMeta metadata, Map<String, String> selector, String portName, int port) {
        super(metadata, selector, portName, port, HEADLESS_CLUSTER_IP, true);
    }
}
