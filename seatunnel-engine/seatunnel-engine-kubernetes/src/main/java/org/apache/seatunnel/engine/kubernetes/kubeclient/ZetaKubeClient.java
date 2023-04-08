/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.kubernetes.kubeclient;

import com.hazelcast.cluster.Endpoint;
import org.apache.seatunnel.engine.kubernetes.KubernetesSpecification;
import org.apache.seatunnel.engine.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.engine.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.engine.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.seatunnel.engine.kubernetes.kubeclient.resources.KubernetesWatch;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * The client to talk with kubernetes. The interfaces will be called both in Client and
 * ResourceManager.
 */
public interface ZetaKubeClient extends AutoCloseable {

    void createComponent(KubernetesSpecification kubernetesJMSpec);

    CompletableFuture<Void> createServerPod(KubernetesPod kubernetesPod);

    CompletableFuture<Void> stopPod(String podName);


    void stopAndCleanupCluster(String clusterId);

    Optional<KubernetesService> getRestService(String clusterId);

    Optional<Endpoint> getRestEndpoint(String clusterId);

    List<KubernetesPod> getPodsWithLabels(Map<String, String> labels);

    KubernetesWatch watchPodsAndDoCallback(
            Map<String, String> labels, WatchCallbackHandler<KubernetesPod> podCallbackHandler)
            throws Exception;

    CompletableFuture<Void> createConfigMap(KubernetesConfigMap configMap);

    Optional<KubernetesConfigMap> getConfigMap(String name);


    CompletableFuture<Boolean> checkAndUpdateConfigMap(
            String configMapName,
            Function<KubernetesConfigMap, Optional<KubernetesConfigMap>> updateFunction);

    KubernetesWatch watchConfigMaps(
            String name, WatchCallbackHandler<KubernetesConfigMap> callbackHandler);


    CompletableFuture<Void> deleteConfigMapsByLabels(Map<String, String> labels);

    CompletableFuture<Void> deleteConfigMap(String configMapName);

    /** Close the Kubernetes client with no exception. */
    void close();

    /**
     * Load pod from template file.
     *
     * @param podTemplateFile The pod template file.
     * @return Return a Kubernetes pod loaded from the template.
     */
    KubernetesPod loadPodFromTemplateFile(File podTemplateFile);

    /** Callback handler for kubernetes resources. */
    interface WatchCallbackHandler<T> {

        void onAdded(List<T> resources);

        void onModified(List<T> resources);

        void onDeleted(List<T> resources);

        void onError(List<T> resources);

        void handleError(Throwable throwable);
    }
}
