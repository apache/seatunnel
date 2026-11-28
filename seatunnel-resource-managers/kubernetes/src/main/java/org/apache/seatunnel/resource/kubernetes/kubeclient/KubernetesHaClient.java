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

import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesService;

import java.util.Map;

/** Optional Kubernetes capability, required before creating an HA application. */
public interface KubernetesHaClient extends KubernetesClient {
    KubernetesService getService(String name);

    /** Updates the observed UID/resourceVersion; a conflict must be re-read before retrying. */
    void updateHaState(KubernetesJob observed, Map<String, String> annotations);

    /** Deletes compute matching this owner's UID, preserving the durable Job and configuration. */
    void deleteHaResources(KubernetesJob owner);

    /** Deletes the observed application with UID preconditions, never a replacement by name. */
    void deleteApplication(String id, String uid);

    /** Removes orphan Pods only when their Job owner reference matches the observed owner UID. */
    void deleteOrphanPods(String id, String ownerUid);

    static KubernetesHaClient require(KubernetesClient client) {
        if (!(client instanceof KubernetesHaClient)) {
            throw new IllegalArgumentException("Kubernetes client does not support Application HA");
        }
        return (KubernetesHaClient) client;
    }
}
