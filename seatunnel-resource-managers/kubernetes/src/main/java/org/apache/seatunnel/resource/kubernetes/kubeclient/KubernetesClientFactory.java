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

package org.apache.seatunnel.resource.kubernetes.kubeclient;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Creates Kubernetes clients from submitter or in-cluster credentials. */
public final class KubernetesClientFactory {
    private static final int API_TIMEOUT_MILLIS = 10_000;

    private KubernetesClientFactory() {}

    /**
     * Creates a namespace-scoped client with bounded API request timeouts.
     *
     * @param options deployment namespace and optional submitter kubeconfig
     * @param inCluster whether only the running pod's service-account credentials may be used
     * @return client owned by the caller
     * @throws IOException if credentials or kubeconfig cannot be loaded
     */
    public static KubernetesClient create(Map<String, String> options, boolean inCluster)
            throws IOException {
        ReadonlyConfig config = ReadonlyConfig.fromMap(new HashMap<String, Object>(options));
        String kubeconfig = config.get(KubernetesOptions.KUBE_CONFIG);
        ApiClient client =
                inCluster
                        ? Config.fromCluster()
                        : kubeconfig == null
                                ? Config.defaultClient()
                                : Config.fromConfig(kubeconfig);
        client.setConnectTimeout(API_TIMEOUT_MILLIS);
        client.setReadTimeout(API_TIMEOUT_MILLIS);
        client.setHttpClient(
                client.getHttpClient()
                        .newBuilder()
                        .callTimeout(API_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                        .build());
        return new DefaultKubernetesClient(client, config.get(KubernetesOptions.NAMESPACE));
    }
}
