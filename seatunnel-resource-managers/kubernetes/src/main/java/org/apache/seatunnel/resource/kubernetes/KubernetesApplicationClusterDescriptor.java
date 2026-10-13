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
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesApplicationSpecification;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesApplicationFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesConfigMapEntry;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.client.config.ClientConfig;
import io.kubernetes.client.openapi.ApiException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Submits one isolated application Job and discovers its control plane on demand. */
final class KubernetesApplicationClusterDescriptor implements ClusterDescriptor<String> {
    private static final Logger LOG =
            LoggerFactory.getLogger(KubernetesApplicationClusterDescriptor.class);

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
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(specification, options);
        String id = KubernetesResourceIdentity.newId(specification.getName());
        String mainClass = SeatunnelKubernetesApplicationCli.class.getName();
        validateUserManagedResources(parameters);
        KubernetesApplicationSpecification applicationSpecification =
                KubernetesApplicationFactory.buildApplicationSpecification(
                        id, mainClass, parameters);
        KubernetesJob job = null;
        try {
            LOG.debug("Creating owner Job {} and its accompanying resources", id);
            job = api.createJob(applicationSpecification.getApplicationJob());
            api.createAccompanyingResources(
                    job, applicationSpecification.getAccompanyingResources());
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
            throw describe(id, failure);
        }
    }

    /**
     * Verifies user-managed ConfigMaps before the first application resource is created.
     *
     * <p>Every {@code kubernetes.configmap.<entry>} reference declares which keys it mounts, so
     * those keys must exist in the referenced ConfigMap. No application-owned resource exists yet,
     * so a failure here never needs compensation.
     *
     * @param parameters resolved application settings
     * @throws Exception if a referenced ConfigMap or one of its mounted keys is missing
     */
    private void validateUserManagedResources(KubernetesApplicationParameters parameters)
            throws Exception {
        for (KubernetesConfigMapEntry entry : parameters.getConfigMaps()) {
            if (!entry.isReference()) {
                continue;
            }
            KubernetesConfigMap configMap =
                    getReadableConfigMap(
                            entry.getReference(), "kubernetes.configmap." + entry.getEntryName());
            if (configMap == null) {
                continue;
            }
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
        } catch (ApiException failure) {
            if (failure.getCode() == 403) {
                LOG.warn(
                        "No permission to verify {} ConfigMap {}; continuing without validation",
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

    /** @return whether a failed request may still have been applied by the API server */
    private static boolean ambiguousOutcome(Exception failure) {
        if (!(failure instanceof ApiException)) {
            return false;
        }
        int code = ((ApiException) failure).getCode();
        return code == 0 || code >= 500;
    }

    private static Exception describe(String id, Exception failure) {
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

    private String awaitMaster(String id, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            KubernetesJob job = api.getJob(id);
            if (job.isFailed() || job.isComplete()) {
                throw new IllegalStateException(
                        "Kubernetes application "
                                + id
                                + " has no live master: "
                                + job.getFailureReason());
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
