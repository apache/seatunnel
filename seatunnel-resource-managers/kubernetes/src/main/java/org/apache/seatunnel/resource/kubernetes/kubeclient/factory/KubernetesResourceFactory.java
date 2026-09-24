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

import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.WorkerSpecification;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesSecret;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.seatunnel.resource.kubernetes.kubeclient.services.HeadlessClusterIPService;

import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1Secret;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Builds the owner Job and its dependent Kubernetes objects without making remote calls.
 *
 * <p>Each method returns a new mutable SDK model. The submitting caller owns that model and must
 * create or delete it through the SDK; creating a model alone allocates no cluster resources.
 */
public final class KubernetesResourceFactory {
    /** Identifies master and worker pods within one application. */
    public static final String ROLE_LABEL = KubernetesConstants.ROLE_LABEL;

    /** File name used to mount the serialized application specification into the master. */
    static final String SPECIFICATION_FILE = KubernetesConstants.SPECIFICATION_FILE;

    private KubernetesResourceFactory() {}

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

    private static V1OwnerReference owner(KubernetesJob job) {
        return new V1OwnerReference()
                .apiVersion(KubernetesConstants.BATCH_API_VERSION)
                .kind(KubernetesConstants.JOB_KIND)
                .name(job.getName())
                .uid(job.getUid())
                .controller(false)
                .blockOwnerDeletion(false);
    }

    /**
     * Builds a suspended, non-restarting master Job with the configured terminal retention period.
     *
     * @param id generated application identifier
     * @param parameters validated image, master resources and optional caller-owned PVC
     * @return Job model to create before its configuration and service; the caller starts it last
     */
    public static KubernetesJob job(String id, KubernetesApplicationParameters parameters) {
        return new KubernetesJob(
                new V1Job()
                        .apiVersion(KubernetesConstants.BATCH_API_VERSION)
                        .kind(KubernetesConstants.JOB_KIND)
                        .metadata(
                                new V1ObjectMeta()
                                        .name(id)
                                        .labels(
                                                ownershipLabels(
                                                        id, KubernetesConstants.MASTER_ROLE)))
                        .spec(
                                new V1JobSpec()
                                        .suspend(true)
                                        .backoffLimit(0)
                                        .completions(1)
                                        .parallelism(1)
                                        .ttlSecondsAfterFinished(parameters.getRetentionSeconds())
                                        .template(
                                                new V1PodTemplateSpec()
                                                        .metadata(
                                                                podMetadata(
                                                                        id,
                                                                        KubernetesConstants
                                                                                .MASTER_ROLE,
                                                                        parameters
                                                                                .getMasterLabels(),
                                                                        parameters
                                                                                .getMasterAnnotations()))
                                                        .spec(
                                                                KubernetesPodFactory.master(
                                                                        id, parameters)))));
    }

    /**
     * Serializes the application into a Secret owned by the already-created Job.
     *
     * @param job owner Job containing its server-assigned UID
     * @param specification immutable job content and deployment options
     * @return mounted configuration model with the submitter's local kubeconfig path removed
     * @throws IOException if in-memory serialization fails
     */
    public static KubernetesSecret secret(KubernetesJob job, ApplicationSpecification specification)
            throws IOException {
        // A submitter's local kubeconfig path must never be used by the in-cluster driver.
        Map<String, String> options = new HashMap<>(specification.getOptions());
        options.remove(KubernetesOptions.KUBE_CONFIG.key());
        ApplicationSpecification localizedSpecification =
                new ApplicationSpecification(
                        specification.getDeployType(),
                        specification.getName(),
                        specification.getJobConfig(),
                        specification.getWorkerCount(),
                        specification.getWorkerSpecification(),
                        options);
        StringWriter serialized = new StringWriter();
        localizedSpecification.write(serialized);
        return new KubernetesSecret(
                new V1Secret()
                        .apiVersion(KubernetesConstants.CORE_API_VERSION)
                        .kind(KubernetesConstants.SECRET_KIND)
                        .type(KubernetesConstants.OPAQUE_SECRET_TYPE)
                        .metadata(
                                metadata(
                                        job, job.getName(), KubernetesConstants.CONFIGURATION_ROLE))
                        .stringData(
                                Collections.singletonMap(
                                        SPECIFICATION_FILE, serialized.toString())));
    }

    /**
     * Builds the headless service owned by the application's master Job.
     *
     * @param job owner Job containing its server-assigned UID
     * @param parameters application master network parameters
     * @return service model selecting only this application's master pod
     */
    public static KubernetesService service(
            KubernetesJob job, KubernetesApplicationParameters parameters) {
        String id = job.getName();
        int port = parameters.getSpecification().getOption(ApplicationOptions.MASTER_PORT);
        return new HeadlessClusterIPService(
                metadata(job, id, KubernetesConstants.MASTER_ROLE),
                ownershipLabels(id, KubernetesConstants.MASTER_ROLE),
                KubernetesConstants.HAZELCAST_PORT_NAME,
                port);
    }

    public static KubernetesPod worker(
            KubernetesJob job,
            String name,
            KubernetesApplicationParameters parameters,
            WorkerSpecification resources,
            String clusterName,
            String masterAddress) {
        return new KubernetesPod(
                new V1Pod()
                        .apiVersion(KubernetesConstants.CORE_API_VERSION)
                        .kind(KubernetesConstants.POD_KIND)
                        .metadata(
                                metadata(job, name, KubernetesConstants.WORKER_ROLE)
                                        .labels(
                                                podLabels(
                                                        job.getName(),
                                                        KubernetesConstants.WORKER_ROLE,
                                                        parameters.getWorkerLabels()))
                                        .annotations(parameters.getWorkerAnnotations()))
                        .spec(
                                KubernetesPodFactory.worker(
                                        parameters, resources, clusterName, masterAddress)));
    }

    private static V1ObjectMeta metadata(KubernetesJob job, String name, String role) {
        return new V1ObjectMeta()
                .name(name)
                .labels(ownershipLabels(job.getName(), role))
                .ownerReferences(Collections.singletonList(owner(job)));
    }

    private static V1ObjectMeta podMetadata(
            String id,
            String role,
            Map<String, String> customLabels,
            Map<String, String> annotations) {
        return new V1ObjectMeta()
                .labels(podLabels(id, role, customLabels))
                .annotations(annotations);
    }

    private static Map<String, String> podLabels(
            String id, String role, Map<String, String> customLabels) {
        return labels(id, role, customLabels);
    }
}
