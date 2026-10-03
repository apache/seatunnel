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

import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesSecret;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesWatch;

import io.kubernetes.client.openapi.ApiException;

import java.util.List;
import java.util.function.Consumer;

/**
 * Namespace-scoped operations required by native SeaTunnel application mode.
 *
 * <p>The interface deliberately exposes SeaTunnel Kubernetes resource wrappers instead of SDK
 * models so submission and worker lifecycle code remain independent of the client library.
 */
public interface KubernetesClient extends AutoCloseable {
    /** Reads an existing ConfigMap before Pods reference it for runtime configuration. */
    KubernetesConfigMap getConfigMap(String name) throws ApiException;

    /** Creates the suspended owner Job and returns its server-assigned metadata. */
    KubernetesJob createJob(KubernetesJob job) throws ApiException;

    /** Reads the durable owner Job for one application. */
    KubernetesJob getJob(String name) throws ApiException;

    /** Starts a previously suspended owner Job without replacing concurrent controller fields. */
    void startJob(String name) throws ApiException;

    /** Creates the Secret containing the localized application specification. */
    void createSecret(KubernetesSecret secret) throws ApiException;

    /** Creates the headless Service used for master discovery. */
    void createService(KubernetesService service) throws ApiException;

    /** Creates one worker Pod owned by the application Job. */
    void createPod(KubernetesPod pod) throws ApiException;

    /** Returns Pods matching a namespace-local label selector. */
    List<KubernetesPod> listPods(String selector) throws ApiException;

    /**
     * Observes matching Pods and emits complete snapshots at a bounded interval.
     *
     * @param selector namespace-local Pod label selector
     * @param intervalMillis interval between snapshots
     * @param listener snapshot callback invoked by the watch thread
     * @param errorHandler terminal API or callback failure handler
     * @return handle that stops observation
     */
    KubernetesWatch watchPods(
            String selector,
            long intervalMillis,
            Consumer<List<KubernetesPod>> listener,
            Consumer<Exception> errorHandler);

    /** Deletes one worker Pod and accepts an already absent Pod. */
    void deletePod(String name) throws ApiException;

    /** Deletes the owner Job and all dependent application resources. */
    void deleteApplication(String id) throws ApiException;

    /** Deletes all worker Pods while preserving the application master. */
    void deleteWorkers(String id) throws ApiException;

    /** Releases client transport resources without deleting Kubernetes objects. */
    @Override
    void close();
}
