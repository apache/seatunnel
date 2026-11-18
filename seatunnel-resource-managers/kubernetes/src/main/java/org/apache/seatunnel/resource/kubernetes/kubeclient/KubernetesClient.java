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

import org.apache.seatunnel.resource.kubernetes.exception.KubernetesClientException;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesSecret;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesWatch;

import java.util.List;

/**
 * Namespace-scoped operations required by native SeaTunnel application mode.
 *
 * <p>The interface deliberately exposes SeaTunnel Kubernetes resource wrappers instead of SDK
 * models so submission and worker lifecycle code remain independent of the client library. Failures
 * are reported as {@link KubernetesClientException}, which keeps the relocated client library out
 * of the API of the shaded artifact.
 */
public interface KubernetesClient extends AutoCloseable {

    /**
     * Reads an existing ConfigMap before Pods reference it for runtime configuration.
     *
     * @throws KubernetesClientException if the ConfigMap is absent or the request fails
     */
    KubernetesConfigMap getConfigMap(String name);

    /**
     * Reads an existing Secret that Pods reference for runtime configuration or key values.
     *
     * @throws KubernetesClientException if the Secret is absent or the request fails
     */
    KubernetesSecret getSecret(String name);

    /**
     * Creates the suspended owner Job and returns its server-assigned metadata.
     *
     * @throws KubernetesClientException if the API request fails
     */
    KubernetesJob createJob(KubernetesJob job);

    /**
     * Creates resources that accompany the already-created owner Job.
     *
     * <p>Every resource receives the owner Job's reference before creation, so deleting the Job
     * removes exactly the resources created for this application.
     *
     * @param owner created Job supplying the owner reference
     * @param resources accompanying resources in creation order
     * @throws KubernetesClientException if any resource cannot be created
     */
    void createAccompanyingResources(KubernetesJob owner, List<KubernetesResource<?>> resources);

    /**
     * Reads the durable owner Job for one application.
     *
     * @throws KubernetesClientException if the Job is absent or the API request fails
     */
    KubernetesJob getJob(String name);

    /**
     * Starts a previously suspended owner Job without replacing concurrent controller fields.
     *
     * @throws KubernetesClientException if the API request fails
     */
    void startJob(String name);

    /**
     * Creates one worker Pod owned by the application Job.
     *
     * @throws KubernetesClientException if the API request fails
     */
    void createPod(KubernetesPod pod);

    /**
     * Returns Pods matching a namespace-local label selector.
     *
     * @throws KubernetesClientException if the API request fails
     */
    List<KubernetesPod> listPods(String selector);

    /**
     * Observes matching Pods and delivers one callback per watch event.
     *
     * @param selector namespace-local Pod label selector
     * @param callback event and failure callback invoked by the watch threads
     * @return handle that stops observation
     */
    KubernetesWatch watchPods(String selector, PodWatchCallback callback);

    /**
     * Deletes one worker Pod and accepts an already absent Pod.
     *
     * @throws KubernetesClientException if deletion fails for any reason other than absence
     */
    void deletePod(String name);

    /**
     * Deletes the owner Job so Kubernetes also removes the resources it owns.
     *
     * @throws KubernetesClientException if the API request fails
     */
    void deleteApplication(String id);

    /**
     * Deletes all worker Pods while preserving the application master.
     *
     * @throws KubernetesClientException if the API request fails
     */
    void deleteWorkers(String id);

    /** Releases client transport resources without deleting Kubernetes objects. */
    @Override
    void close();
}
