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

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Identity contract of one SeaTunnel deployment: how its Kubernetes resources are named, labelled,
 * selected and owned.
 *
 * <p>A deployment is the owner of a set of resources; application mode is one today, a session
 * cluster can reuse the same contract later. The id generated here becomes the workload name and
 * the value of the ownership label every owned resource carries. The client, the in-cluster
 * resource manager and the decorator chain all derive their selectors and metadata from this
 * contract, so discovery and cleanup cannot drift apart.
 */
public final class KubernetesResourceIdentity {

    private KubernetesResourceIdentity() {}

    /**
     * Generates a fresh DNS-compatible id with room for generated pod suffixes.
     *
     * @param name display name used as a sanitized prefix
     * @return a unique id suitable for Kubernetes resource names
     */
    public static String newId(String name) {
        String prefix =
                name.toLowerCase(Locale.ROOT)
                        .replaceAll("[^a-z0-9-]", "-")
                        .replaceAll("^-+|-+$", "");
        if (prefix.isEmpty()) {
            prefix = "seatunnel";
        }
        prefix = prefix.substring(0, Math.min(prefix.length(), 35)).replaceAll("-+$", "");
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 12);
    }

    /**
     * Selects every resource of one deployment regardless of its role.
     *
     * @param id generated deployment id
     * @return equality selector for the ownership label
     */
    public static String selector(String id) {
        return KubernetesConstants.APPLICATION_LABEL + "=" + id;
    }

    /**
     * Selects the resources of one deployment that have the given role.
     *
     * @param id generated deployment id
     * @param role role
     * @return equality selector for the ownership label and role
     */
    public static String selector(String id, String role) {
        return selector(id) + "," + KubernetesConstants.ROLE_LABEL + "=" + role;
    }

    /**
     * Builds the ownership metadata of one accompanying resource of this deployment.
     *
     * <p>The owner reference is added centrally when the resource is created, because only the API
     * server knows the Job UID.
     *
     * @param ownerId generated deployment id used as the ownership label value
     * @param name resource name
     * @param role role label distinguishing configuration objects from Pods
     * @return metadata carrying SeaTunnel's ownership labels
     */
    public static V1ObjectMeta metadata(String ownerId, String name, String role) {
        return new V1ObjectMeta().name(name).labels(ownershipLabels(ownerId, role));
    }

    /**
     * Builds the metadata of the master Pod template inside the owner Job.
     *
     * @param ownerId generated deployment id
     * @param role role label of the Pod
     * @param customLabels user-configured labels
     * @param annotations user-configured annotations
     * @return Pod metadata without an owner reference, because the Job owns its template
     */
    public static V1ObjectMeta podMetadata(
            String ownerId,
            String role,
            Map<String, String> customLabels,
            Map<String, String> annotations) {
        return new V1ObjectMeta()
                .labels(labels(ownerId, role, customLabels))
                .annotations(annotations);
    }

    /**
     * Builds the metadata of a Pod owned by the deployment's Job.
     *
     * @param owner owner Job; its server-assigned UID is required
     * @param name Pod name
     * @param role role label of the Pod
     * @param customLabels user-configured labels
     * @param annotations user-configured annotations
     * @return Pod metadata carrying the owner reference
     */
    public static V1ObjectMeta podMetadata(
            KubernetesJob owner,
            String name,
            String role,
            Map<String, String> customLabels,
            Map<String, String> annotations) {
        return podMetadata(owner.getName(), role, customLabels, annotations)
                .name(name)
                .ownerReferences(Collections.singletonList(ownerReference(owner)));
    }

    private static Map<String, String> labels(
            String id, String role, Map<String, String> customLabels) {
        Map<String, String> result = new HashMap<>();
        result.put(KubernetesConstants.APPLICATION_LABEL, id);
        result.put(KubernetesConstants.ROLE_LABEL, role);
        result.putAll(customLabels);
        return result;
    }

    private static Map<String, String> ownershipLabels(String id, String role) {
        return labels(id, role, Collections.emptyMap());
    }

    private static V1OwnerReference ownerReference(KubernetesJob job) {
        return new V1OwnerReference()
                .apiVersion(KubernetesConstants.BATCH_API_VERSION)
                .kind(KubernetesConstants.JOB_KIND)
                .name(job.getName())
                .uid(job.getUid())
                .controller(false)
                .blockOwnerDeletion(false);
    }
}
