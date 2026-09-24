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

package org.apache.seatunnel.resource.kubernetes.client;

import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.client.ApplicationClient;
import org.apache.seatunnel.resource.core.client.ApplicationDeployer;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;

import io.kubernetes.client.openapi.ApiException;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Deploys a suspended owner Job, localizes configuration, then starts its control plane. */
final class KubernetesApplicationDeployer implements ApplicationDeployer {
    private final KubernetesClient api;

    KubernetesApplicationDeployer(KubernetesClient api) {
        this.api = api;
    }

    /**
     * Creates and starts one isolated application, rolling back all partial startup resources.
     *
     * @param specification resolved job content and fixed resource requirements
     * @return handle sharing this deployer's SDK connection; closing it does not cancel the job
     * @throws Exception on invalid options, admission errors or configuration localization failures
     */
    @Override
    public ApplicationClient deploy(ApplicationSpecification specification) throws Exception {
        if (specification.getDeployType() != DeployType.KUBERNETES) {
            throw new IllegalArgumentException("Expected Kubernetes specification");
        }
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(specification);
        if (parameters.getConfigMap() != null) {
            // Fail before creating application-owned resources when the user-owned runtime
            // configuration does not exist or is not readable by the submitting client.
            api.getConfigMap(parameters.getConfigMap());
        }
        String id = KubernetesResourceFactory.newId(specification.getName());
        boolean ownerCreated = false;
        try {
            KubernetesJob job = api.createJob(KubernetesResourceFactory.job(id, parameters));
            ownerCreated = true;
            api.createSecret(KubernetesResourceFactory.secret(job, specification));
            api.createService(KubernetesResourceFactory.service(job, parameters));
            api.startJob(id);
            KubernetesApplicationClient client =
                    new KubernetesApplicationClient(
                            api, new ApplicationId(DeployType.KUBERNETES, id));
            awaitMaster(client, specification.getStartupTimeoutMillis());
            return client;
        } catch (Exception failure) {
            if (ownerCreated
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

    private void awaitMaster(KubernetesApplicationClient client, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            ApplicationStatus status = client.getStatus();
            if (status == ApplicationStatus.RUNNING || status == ApplicationStatus.SUCCEEDED) {
                return;
            }
            if (status == ApplicationStatus.FAILED || status == ApplicationStatus.CANCELED) {
                throw new IllegalStateException(
                        "Application master failed before startup: "
                                + client.getApplicationId().getId());
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new TimeoutException(
                        "Timed out waiting for Kubernetes application master "
                                + client.getApplicationId().getId());
            }
            TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(200)));
        }
    }

    /**
     * Creates a status/cancel handle without connecting to the application's master.
     *
     * @param applicationId Kubernetes Job name and deployment target
     * @param options deployment options; the connection and namespace were selected by the factory
     * @return handle sharing this deployer's SDK connection
     */
    @Override
    public ApplicationClient retrieve(ApplicationId applicationId, Map<String, String> options) {
        if (applicationId.getDeployType() != DeployType.KUBERNETES) {
            throw new IllegalArgumentException("Expected Kubernetes application id");
        }
        return new KubernetesApplicationClient(api, applicationId);
    }

    /** Releases the SDK connection without canceling or deleting any application. */
    @Override
    public void close() {
        api.close();
    }
}
