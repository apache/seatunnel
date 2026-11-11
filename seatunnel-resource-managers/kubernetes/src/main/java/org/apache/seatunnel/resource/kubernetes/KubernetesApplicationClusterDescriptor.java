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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.deployment.ClusterDescriptor;
import org.apache.seatunnel.engine.client.deployment.SeatunnelClientProvider;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.resource.kubernetes.cli.SeatunnelKubernetesApplicationCli;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationClient;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesConfigMapEntry;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesEnvFromReference;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesEnvironmentVariable;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.exception.KubernetesClientException;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.ConfigMapDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.EnvironmentDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.HeadlessServiceDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.KubernetesStepDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.SecretDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.SpecificationEnvDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.ApplicationJobFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesSecret;

import com.hazelcast.client.config.ClientConfig;
import io.kubernetes.client.openapi.models.V1Pod;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Submits one isolated application Job and discovers its control plane on demand. */
@Slf4j
final class KubernetesApplicationClusterDescriptor implements ClusterDescriptor<String> {

    private final KubernetesClient api;
    private final ReadonlyConfig options;

    KubernetesApplicationClusterDescriptor(KubernetesClient api, Map<String, String> options) {
        this.api = api;
        this.options = ReadonlyConfig.fromMap(new HashMap<>(options));
    }

    private void validateApplicationId(String applicationId) {
        if (applicationId == null || applicationId.trim().isEmpty()) {
            throw new IllegalArgumentException("Kubernetes Job name must not be empty");
        }
    }

    /**
     * Creates and starts one isolated application without waiting for its master.
     *
     * <p>Image pull, admission and scheduling therefore cannot fail the submission; callers observe
     * the runtime outcome through {@link #getApplicationStatus(String)} or {@link
     * #retrieve(String)}. Only resources created by this invocation are compensated when creation
     * fails.
     *
     * @param specification resolved job content and fixed resource requirements
     * @return the native Kubernetes Job name
     * @throws Exception on invalid options or admission errors
     */
    @Override
    public String deployApplication(ApplicationSpecification specification) throws Exception {
        KubernetesApplicationConfig parameters =
                KubernetesApplicationConfig.from(specification, options);
        String id = KubernetesResourceIdentity.newId(specification.getName());
        String mainClass = SeatunnelKubernetesApplicationCli.class.getName();
        validateUserManagedResources(parameters, id);
        List<KubernetesResource<?>> application = assembleApplication(id, mainClass, parameters);
        KubernetesJob job = null;
        try {
            log.debug("Creating owner Job {} and its accompanying resources", id);
            job = api.createJob((KubernetesJob) application.remove(0));
            api.createAccompanyingResources(job, application);
            log.debug("Starting Job {}", id);
            api.startJob(job.getName());
            return job.getName();
        } catch (Exception failure) {
            log.warn("Kubernetes application {} deployment failed", id, failure);
            if (job != null || ambiguousOutcome(failure)) {
                // Deleting the owner removes exactly the resources created for this application.
                try {
                    api.deleteApplication(id);
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw describe(id, failure);
        }
    }

    /**
     * Assembles the application's resources through the ordered decorator chain.
     *
     * <p>The master Pod skeleton is created first, the chain decorates it and contributes the
     * accompanying resources, and the decorated Pod is finally wrapped into the suspended owner
     * Job. The owner Job is returned first because the caller must create it before the resources
     * that carry its owner reference.
     *
     * @param id generated application Job name
     * @param mainClass master entrypoint class
     * @param parameters resolved application and Kubernetes settings
     * @return the application's resources in creation order: the owner Job first, then its
     *     accompanying resources
     */
    static List<KubernetesResource<?>> assembleApplication(
            String id, String mainClass, KubernetesApplicationConfig parameters) {
        V1Pod masterPod = ApplicationJobFactory.createMasterPod(id, mainClass, parameters);
        List<KubernetesStepDecorator> decorators =
                Arrays.asList(
                        new SpecificationEnvDecorator(parameters),
                        new EnvironmentDecorator(parameters),
                        new SecretDecorator(id, parameters),
                        new ConfigMapDecorator(id, parameters),
                        new HeadlessServiceDecorator(id, parameters));
        List<KubernetesResource<?>> resources = new ArrayList<>();
        for (KubernetesStepDecorator decorator : decorators) {
            masterPod = decorator.decorate(masterPod);
            resources.addAll(decorator.build());
        }
        resources.add(0, ApplicationJobFactory.createOwnerJob(id, parameters, masterPod));
        return resources;
    }

    /**
     * Verifies every user-managed ConfigMap, Secret and environment reference before the first
     * application resource is created, so a misconfiguration never needs compensation.
     *
     * @param parameters resolved application settings
     * @param applicationId generated Job name, which also names a Secret without an explicit name
     * @throws Exception if a referenced object or key is missing, or the API fails otherwise
     */
    private void validateUserManagedResources(
            KubernetesApplicationConfig parameters, String applicationId) throws Exception {
        // A created Secret or ConfigMap does not exist before this submission, so references to it
        // are validated against the declared data keys instead of the API.
        String createdSecret = createdSecretName(parameters, applicationId);
        Map<String, KubernetesConfigMapEntry> createdConfigMaps =
                createdConfigMaps(parameters, applicationId);
        for (KubernetesConfigMapEntry entry : parameters.getConfigMaps()) {
            if (!entry.isReference()) {
                continue;
            }
            String option = KubernetesConstants.CONFIG_MAP_PREFIX + entry.getEntryName();
            KubernetesConfigMap configMap = getReadableConfigMap(entry.getReference(), option);
            if (configMap == null) {
                continue;
            }
            for (String key : entry.getMountPaths().keySet()) {
                if (!containsKey(configMap, key)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "%s mounts missing key %s from %s %s",
                                    option,
                                    key,
                                    KubernetesConstants.CONFIG_MAP_KIND,
                                    entry.getReference()));
                }
            }
        }
        if (parameters.getApplicationSecretRef() != null) {
            getReadableSecret(
                    parameters.getApplicationSecretRef(), KubernetesOptions.SECRET_REF.key());
        }
        for (KubernetesEnvironmentVariable variable : parameters.getEnvironmentVariables()) {
            validateEnvironmentVariable(variable, parameters, createdSecret, createdConfigMaps);
        }
        for (KubernetesEnvFromReference reference : parameters.getEnvFromReferences()) {
            if (reference.getSource() == KubernetesEnvFromReference.Source.SECRET) {
                if (!reference.getName().equals(createdSecret)) {
                    getReadableSecret(reference.getName(), KubernetesConstants.ENV_FROM_SECRET);
                }
            } else if (!createdConfigMaps.containsKey(reference.getName())) {
                getReadableConfigMap(reference.getName(), KubernetesConstants.ENV_FROM_CONFIGMAP);
            }
        }
    }

    /**
     * @return name of the Secret this submission creates from {@code kubernetes.secret.data}, or
     *     {@code null} when no Secret is created
     */
    private static String createdSecretName(
            KubernetesApplicationConfig parameters, String applicationId) {
        if (parameters.getApplicationSecretData().isEmpty()) {
            return null;
        }
        return parameters.resolvedApplicationSecretName(applicationId);
    }

    /** @return ConfigMaps created by this submission, keyed by their resolved name */
    private static Map<String, KubernetesConfigMapEntry> createdConfigMaps(
            KubernetesApplicationConfig parameters, String applicationId) {
        Map<String, KubernetesConfigMapEntry> created = new HashMap<>();
        for (KubernetesConfigMapEntry entry : parameters.getConfigMaps()) {
            if (!entry.isReference()) {
                created.put(entry.createdName(applicationId), entry);
            }
        }
        return created;
    }

    /**
     * Verifies one {@code kubernetes.env} reference before the first resource is created.
     *
     * @param variable configured environment variable, literal or reference
     * @param parameters resolved application settings that declare the created Secret keys
     * @param createdSecret name of the Secret this submission creates, or {@code null}
     * @param createdConfigMaps ConfigMaps created by this submission, keyed by their resolved name
     * @throws Exception if the referenced Secret or ConfigMap or one of its keys is missing
     */
    private void validateEnvironmentVariable(
            KubernetesEnvironmentVariable variable,
            KubernetesApplicationConfig parameters,
            String createdSecret,
            Map<String, KubernetesConfigMapEntry> createdConfigMaps)
            throws Exception {
        String option = KubernetesConstants.ENV_PREFIX + variable.getName();
        if (variable.isSecret()) {
            if (variable.getSecretName().equals(createdSecret)) {
                if (!parameters.getApplicationSecretData().containsKey(variable.getKey())) {
                    throw missingKey(
                            option,
                            variable.getKey(),
                            KubernetesConstants.SECRET_KIND,
                            createdSecret);
                }
                return;
            }
            KubernetesSecret secret = getReadableSecret(variable.getSecretName(), option);
            if (secret != null && !containsKey(secret, variable.getKey())) {
                throw missingKey(
                        option,
                        variable.getKey(),
                        KubernetesConstants.SECRET_KIND,
                        variable.getSecretName());
            }
        } else if (variable.isConfigMap()) {
            KubernetesConfigMapEntry created = createdConfigMaps.get(variable.getConfigMapName());
            if (created != null) {
                if (!created.getData().containsKey(variable.getKey())) {
                    throw missingKey(
                            option,
                            variable.getKey(),
                            KubernetesConstants.CONFIG_MAP_KIND,
                            variable.getConfigMapName());
                }
                return;
            }
            KubernetesConfigMap configMap =
                    getReadableConfigMap(variable.getConfigMapName(), option);
            if (configMap != null && !containsKey(configMap, variable.getKey())) {
                throw missingKey(
                        option,
                        variable.getKey(),
                        KubernetesConstants.CONFIG_MAP_KIND,
                        variable.getConfigMapName());
            }
        }
    }

    /** @return error for one environment reference whose key is missing in the referenced object */
    private static IllegalArgumentException missingKey(
            String option, String key, String kind, String name) {
        return new IllegalArgumentException(
                String.format("%s references missing key %s in %s %s", option, key, kind, name));
    }

    /**
     * Reads a user-managed ConfigMap for validation.
     *
     * <p>A missing ConfigMap or key is a configuration error reported before creation. When the
     * submitting identity cannot read the ConfigMap, validation is skipped with a warning because
     * the Pod may still reference it through its own service account.
     *
     * @param name ConfigMap name
     * @param option option that references it, used in the warning
     * @return the ConfigMap, or {@code null} when the submitter is not allowed to read it
     * @throws Exception if the ConfigMap is missing or the API fails for another reason
     */
    private KubernetesConfigMap getReadableConfigMap(String name, String option) throws Exception {
        try {
            return api.getConfigMap(name);
        } catch (KubernetesClientException failure) {
            if (failure.getCode() == 403) {
                log.warn(
                        "No permission to verify {} ConfigMap {}; continuing without validation",
                        option,
                        name);
                return null;
            }
            throw failure;
        }
    }

    /**
     * Reads a user-managed Secret for validation.
     *
     * <p>A missing Secret is a configuration error reported before creation. When the submitting
     * identity cannot read the Secret, validation is skipped with a warning because the Pod may
     * still reference it through its own service account.
     *
     * @param name Secret name
     * @param option option that references it, used in the warning
     * @return the Secret, or {@code null} when the submitter is not allowed to read it
     * @throws Exception if the Secret is missing or the API fails for another reason
     */
    private KubernetesSecret getReadableSecret(String name, String option) throws Exception {
        try {
            return api.getSecret(name);
        } catch (KubernetesClientException failure) {
            if (failure.getCode() == 403) {
                log.warn(
                        "No permission to verify {} Secret {}; continuing without validation",
                        option,
                        name);
                return null;
            }
            throw failure;
        }
    }

    private static boolean containsKey(KubernetesConfigMap configMap, String key) {
        return (configMap.getInternalResource().getData() != null
                        && configMap.getInternalResource().getData().containsKey(key))
                || (configMap.getInternalResource().getBinaryData() != null
                        && configMap.getInternalResource().getBinaryData().containsKey(key));
    }

    private static boolean containsKey(KubernetesSecret secret, String key) {
        return (secret.getInternalResource().getData() != null
                        && secret.getInternalResource().getData().containsKey(key))
                || (secret.getInternalResource().getStringData() != null
                        && secret.getInternalResource().getStringData().containsKey(key));
    }

    /** @return whether a failed request may still have been applied by the API server */
    private static boolean ambiguousOutcome(Exception failure) {
        if (!(failure instanceof KubernetesClientException)) {
            return false;
        }
        int code = ((KubernetesClientException) failure).getCode();
        return code == 0 || code >= 500;
    }

    private static Exception describe(String id, Exception failure) {
        if (!(failure instanceof KubernetesClientException)) {
            return failure;
        }
        KubernetesClientException clientFailure = (KubernetesClientException) failure;
        return new KubernetesClientException(
                clientFailure.getCode(),
                String.format(
                        "Kubernetes application deployment failed for %s (HTTP %d)",
                        id, clientFailure.getCode()),
                clientFailure);
    }

    private String awaitMaster(String id, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            KubernetesJob job = api.getJob(id);
            if (job.isFailed() || job.isComplete()) {
                throw new IllegalStateException(
                        String.format(
                                "Kubernetes application %s has no live master: %s",
                                id, job.getFailureReason()));
            }
            String selector =
                    KubernetesResourceIdentity.selector(id, KubernetesConstants.MASTER_ROLE);
            for (KubernetesPod pod : api.listPods(selector)) {
                if (pod.isRunning() && !pod.isTerminating()) {
                    String host = pod.getInternalResource().getStatus().getPodIP();
                    if (host != null && !host.isEmpty()) {
                        return host;
                    }
                }
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new TimeoutException(
                        "Timed out waiting for Kubernetes application master " + id);
            }
            TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(200)));
        }
    }

    /** Discovers the running master Pod without creating an Engine client. */
    @Override
    public SeatunnelClientProvider retrieve(String applicationId) throws Exception {
        validateApplicationId(applicationId);
        long timeout = options.get(ApplicationOptions.STARTUP_TIMEOUT_MILLIS);
        String host = awaitMaster(applicationId, timeout);
        String address = masterAddress(host, options.get(ApplicationOptions.MASTER_PORT));
        ClientConfig config = ConfigProvider.locateAndGetClientConfig();
        config.setClusterName(SeatunnelApplicationConfig.clusterName(applicationId));
        config.getNetworkConfig().setAddresses(Collections.singletonList(address));
        config.getConnectionStrategyConfig()
                .getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(timeout);
        return () -> new SeaTunnelClient(config);
    }

    private static String masterAddress(String host, int port) {
        String formattedHost =
                host.contains(":") && !host.startsWith("[") ? "[" + host + "]" : host;
        return formattedHost + ":" + port;
    }

    @Override
    public ApplicationStatus getApplicationStatus(String applicationId) throws Exception {
        return new KubernetesApplicationClient(api, applicationId).getStatus();
    }

    @Override
    public void cancelApplication(String applicationId) throws Exception {
        validateApplicationId(applicationId);
        api.deleteApplication(applicationId);
    }

    /** Releases the SDK connection without canceling or deleting any application. */
    @Override
    public void close() {
        api.close();
    }
}
