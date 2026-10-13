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

package org.apache.seatunnel.resource.kubernetes;

import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesApplicationSpecification;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesApplicationFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesConfigMapEntry;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.openapi.ApiException;

/**
 * Creates one application from its decorator-built specification and starts it.
 *
 * <p>The owner Job is created first; every accompanying resource receives its owner reference
 * before creation. When a step fails after the Job exists, the Job is deleted and Kubernetes
 * removes exactly the dependent resources created for this application, so a conflict with a
 * pre-existing object can never delete that object.
 */
final class KubernetesApplicationDeployment {
    private static final Logger LOG =
            LoggerFactory.getLogger(KubernetesApplicationDeployment.class);

    private final KubernetesClient api;
    private final String id;
    private final String mainClass;
    private final KubernetesApplicationParameters parameters;

    KubernetesApplicationDeployment(
            KubernetesClient api,
            String id,
            String mainClass,
            KubernetesApplicationParameters parameters) {
        this.api = api;
        this.id = id;
        this.mainClass = mainClass;
        this.parameters = parameters;
    }

    /**
     * Builds, creates and starts the application resources.
     *
     * @return the Kubernetes application Job name
     * @throws Exception if the specification cannot be built or a resource cannot be created; a
     *     partially created application is compensated before the failure is rethrown
     */
    String deploy() throws Exception {
        KubernetesApplicationSpecification specification =
                KubernetesApplicationFactory.buildApplicationSpecification(
                        id, mainClass, parameters);
        validateUserManagedResources();
        KubernetesJob job = null;
        try {
            LOG.debug("Creating owner Job {} and its accompanying resources", id);
            job = api.createJob(specification.getApplicationJob());
            api.createAccompanyingResources(job, specification.getAccompanyingResources());
            LOG.debug("Starting Job {}", id);
            api.startJob(job.getName());
            return job.getName();
        } catch (Exception failure) {
            LOG.warn("Kubernetes application {} deployment failed", id, failure);
            if (job != null || ambiguousOutcome(failure)) {
                // Deleting the owner removes exactly the resources created for this application.
                try {
                    api.deleteApplication(id);
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw describe(failure);
        }
    }

    /**
     * Verifies user-managed ConfigMaps before the first application resource is created.
     *
     * <p>The legacy {@code kubernetes.config-map} only needs to exist. Every {@code
     * kubernetes.configmap.<entry>} reference additionally declares which keys it mounts, so those
     * keys must exist in the referenced ConfigMap. No application-owned resource exists yet, so a
     * failure here never needs compensation.
     *
     * @throws Exception if a referenced ConfigMap or one of its mounted keys is missing
     */
    private void validateUserManagedResources() throws Exception {
        if (parameters.getConfigMap() != null) {
            api.getConfigMap(parameters.getConfigMap());
        }
        for (KubernetesConfigMapEntry entry : parameters.getConfigMaps()) {
            if (!entry.isReference()) {
                continue;
            }
            KubernetesConfigMap configMap = api.getConfigMap(entry.getReference());
            for (String key : entry.getMountPaths().keySet()) {
                if (!containsKey(configMap, key)) {
                    throw new IllegalArgumentException(
                            "kubernetes.configmap."
                                    + entry.getEntryName()
                                    + " mounts missing key "
                                    + key
                                    + " from ConfigMap "
                                    + entry.getReference());
                }
            }
        }
    }

    private static boolean containsKey(KubernetesConfigMap configMap, String key) {
        return (configMap.getInternalResource().getData() != null
                        && configMap.getInternalResource().getData().containsKey(key))
                || (configMap.getInternalResource().getBinaryData() != null
                        && configMap.getInternalResource().getBinaryData().containsKey(key));
    }

    /** @return whether a failed request may still have been applied by the API server */
    private static boolean ambiguousOutcome(Exception failure) {
        if (!(failure instanceof ApiException)) {
            return false;
        }
        int code = ((ApiException) failure).getCode();
        return code == 0 || code >= 500;
    }

    private Exception describe(Exception failure) {
        if (!(failure instanceof ApiException)) {
            return failure;
        }
        ApiException apiFailure = (ApiException) failure;
        return new ApiException(
                "Kubernetes application deployment failed for "
                        + id
                        + " (HTTP "
                        + apiFailure.getCode()
                        + ")",
                apiFailure,
                apiFailure.getCode(),
                apiFailure.getResponseHeaders(),
                apiFailure.getResponseBody());
    }
}
