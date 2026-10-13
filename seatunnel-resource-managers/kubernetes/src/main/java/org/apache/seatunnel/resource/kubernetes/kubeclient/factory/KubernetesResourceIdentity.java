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

package org.apache.seatunnel.resource.kubernetes.kubeclient.factory;

import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Names, labels, selectors and ownership metadata shared by the application decorators.
 *
 * <p>Resources themselves are created by the decorator chain; this class only describes how they
 * are named and identified, so the client, the resource manager and every decorator agree on the
 * labels used for discovery and cleanup.
 */
public final class KubernetesResourceIdentity {
    /** Identifies master and worker pods within one application. */
    public static final String ROLE_LABEL = KubernetesConstants.ROLE_LABEL;

    private KubernetesResourceIdentity() {}

    /**
     * Generates a fresh DNS-compatible Job name with room for generated pod suffixes.
     *
     * @param name application display name used as a sanitized prefix
     * @return a unique application identifier suitable for Kubernetes resource names
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
     * Selects resources belonging to one application regardless of their role.
     *
     * @param id generated application Job name
     * @return equality selector for the application's ownership label
     */
    public static String selector(String id) {
        return KubernetesConstants.APPLICATION_LABEL + "=" + id;
    }

    /**
     * Selects resources belonging to an application and specifies their role.
     *
     * @param id generated application Job name
     * @param role role
     * @return equality selector for the application's ownership label and role
     */
    public static String selector(String id, String role) {
        return selector(id) + "," + ROLE_LABEL + "=" + role;
    }

    /**
     * Builds the ownership metadata of an application-owned accompanying resource.
     *
     * <p>The owner reference is added centrally when the resource is created, because only the API
     * server knows the Job UID.
     *
     * @param name resource name
     * @param role role label distinguishing configuration objects from Pods
     * @return metadata carrying SeaTunnel's ownership labels
     */
    public static V1ObjectMeta metadata(String name, String role) {
        return new V1ObjectMeta().name(name).labels(ownershipLabels(name, role));
    }

    /**
     * Builds the metadata of the master Pod template inside the owner Job.
     *
     * @param id generated application Job name
     * @param role role label of the Pod
     * @param customLabels user-configured labels
     * @param annotations user-configured annotations
     * @return Pod metadata without an owner reference, because the Job owns its template
     */
    public static V1ObjectMeta podMetadata(
            String id,
            String role,
            Map<String, String> customLabels,
            Map<String, String> annotations) {
        return new V1ObjectMeta().labels(labels(id, role, customLabels)).annotations(annotations);
    }

    /**
     * Builds the metadata of a Pod owned by the application Job.
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
        result.put(ROLE_LABEL, role);
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
