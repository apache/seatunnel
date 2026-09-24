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

import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationResult;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.client.ApplicationClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;

import io.kubernetes.client.openapi.ApiException;

/** Reads durable Kubernetes Job state without connecting to the application master. */
final class KubernetesApplicationClient implements ApplicationClient {
    private final KubernetesClient api;
    private final ApplicationId applicationId;
    private volatile boolean canceled;

    KubernetesApplicationClient(KubernetesClient api, ApplicationId applicationId) {
        this.api = api;
        this.applicationId = applicationId;
    }

    /** @return the generated Kubernetes Job identity */
    @Override
    public ApplicationId getApplicationId() {
        return applicationId;
    }

    /**
     * @return durable Job state, or UNKNOWN after deletion or retention expiry
     * @throws Exception when Kubernetes cannot be queried
     */
    @Override
    public ApplicationStatus getStatus() throws Exception {
        return getResult().getStatus();
    }

    /**
     * Resolves native Job conditions; the method does not expose job configuration or Pod logs.
     *
     * @return current state and Kubernetes reason, when available
     * @throws Exception on API errors other than a missing Job
     */
    @Override
    public ApplicationResult getResult() throws Exception {
        if (canceled) {
            return new ApplicationResult(
                    applicationId, ApplicationStatus.CANCELED, "Application resources deleted");
        }
        try {
            KubernetesJob job = api.getJob(applicationId.getId());
            if (job.isFailed()) {
                return new ApplicationResult(
                        applicationId, ApplicationStatus.FAILED, job.getFailureReason());
            }
            if (job.isComplete()) {
                return new ApplicationResult(applicationId, ApplicationStatus.SUCCEEDED, null);
            }
            ApplicationStatus state = ApplicationStatus.DEPLOYING;
            if (job.isActive()) {
                for (KubernetesPod pod :
                        api.listPods(
                                KubernetesResourceFactory.selector(applicationId.getId())
                                        + ","
                                        + KubernetesResourceFactory.ROLE_LABEL
                                        + "=master")) {
                    if (pod.isRunning()) {
                        state = ApplicationStatus.RUNNING;
                        break;
                    }
                }
            }
            return new ApplicationResult(applicationId, state, null);
        } catch (ApiException e) {
            if (e.getCode() != 404) {
                throw e;
            }
            return new ApplicationResult(
                    applicationId,
                    ApplicationStatus.UNKNOWN,
                    "Job does not exist or its retention period expired");
        }
    }

    /**
     * Deletes the owner and dependent resources; callers may retry after a partial API failure.
     *
     * @throws Exception if any resource could not be deleted
     */
    @Override
    public void cancel() throws Exception {
        api.deleteApplication(applicationId.getId());
        canceled = true;
    }

    /**
     * The deployer owns the shared SDK connection; closing a handle never cancels its application.
     */
    @Override
    public void close() {}
}
