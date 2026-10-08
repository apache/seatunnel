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
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;

import com.hazelcast.client.config.ClientConfig;
import io.kubernetes.client.openapi.ApiException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Deploys a suspended owner Job, localizes configuration, then starts its control plane. */
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
     * Creates and starts one isolated application, rolling back all partial startup resources.
     *
     * @param specification resolved job content and fixed resource requirements
     * @return the native Kubernetes Job name
     * @throws Exception on invalid options, admission errors or configuration localization failures
     */
    @Override
    public String deployApplication(ApplicationSpecification specification) throws Exception {
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(specification, options);
        if (parameters.getConfigMap() != null) {
            // Fail before creating application-owned resources when the user-owned runtime
            // configuration does not exist or is not readable by the submitting client.
            api.getConfigMap(parameters.getConfigMap());
        }
        String id = KubernetesResourceFactory.newId(specification.getName());
        String mainClass = SeatunnelKubernetesApplicationCli.class.getName();
        KubernetesJob job = null;
        try {

            job = api.createJob(KubernetesResourceFactory.job(id, mainClass, parameters));
            api.createSecret(KubernetesResourceFactory.secret(job, parameters));
            api.createService(KubernetesResourceFactory.service(job, parameters));
            api.startJob(id);
            awaitDeployment(id, specification.getStartupTimeoutMillis());
            return id;
        } catch (Exception failure) {
            if (job == null
                    || job.isFailed()
                    || !(failure instanceof ApiException)
                    || ((ApiException) failure).getCode() != 409) {
                try {
                    api.deleteApplication(id);
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            if (failure instanceof ApiException) {
                ApiException apiFailure = (ApiException) failure;
                throw new ApiException(
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
            throw failure;
        }
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
            for (KubernetesPod pod :
                    api.listPods(
                            KubernetesResourceFactory.selector(id)
                                    + ","
                                    + KubernetesResourceFactory.ROLE_LABEL
                                    + "=master")) {
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
        String address =
                (host.contains(":") && !host.startsWith("[") ? "[" + host + "]" : host)
                        + ":"
                        + options.get(ApplicationOptions.MASTER_PORT);
        ClientConfig config = ConfigProvider.locateAndGetClientConfig();
        config.setClusterName(SeatunnelApplicationConfig.clusterName(applicationId));
        config.getNetworkConfig().setAddresses(Collections.singletonList(address));
        config.getConnectionStrategyConfig()
                .getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(timeout);
        return () -> new SeaTunnelClient(config);
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

    private void awaitDeployment(String applicationId, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            ApplicationStatus status = getApplicationStatus(applicationId);
            if (status == ApplicationStatus.RUNNING || status == ApplicationStatus.SUCCEEDED) {
                return;
            }
            if (status.isTerminal()) {
                throw new IllegalStateException(
                        "Kubernetes application " + applicationId + " failed to start: " + status);
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new TimeoutException(
                        "Timed out waiting for Kubernetes application master " + applicationId);
            }
            TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(200)));
        }
    }

    /** Releases the SDK connection without canceling or deleting any application. */
    @Override
    public void close() {
        api.close();
    }
}
