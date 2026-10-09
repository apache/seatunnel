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

import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;

import io.kubernetes.client.openapi.ApiException;

import java.util.List;

/** Reads durable Kubernetes Job state without connecting to the application master. */
public final class KubernetesApplicationClient implements AutoCloseable {
    private final KubernetesClient api;
    private final String applicationId;
    private volatile boolean canceled;

    public KubernetesApplicationClient(KubernetesClient api, String applicationId) {
        this.api = api;
        this.applicationId = applicationId;
    }

    /** @return the generated Kubernetes Job identity */
    public String getClusterId() {
        return applicationId;
    }

    /**
     * @return durable Job state, or UNKNOWN after deletion or retention expiry
     * @throws Exception when Kubernetes cannot be queried
     */
    public ApplicationStatus getStatus() throws Exception {
        if (canceled) {
            return ApplicationStatus.CANCELED;
        }
        try {
            KubernetesJob job = api.getJob(applicationId);
            if (job.isFailed()) {
                return ApplicationStatus.FAILED;
            }
            if (job.isComplete()) {
                return ApplicationStatus.SUCCEEDED;
            }
            ApplicationStatus state = ApplicationStatus.DEPLOYING;
            if (job.isActive()) {
                String selector =
                        KubernetesResourceFactory.selector(
                                applicationId, KubernetesConstants.WORKER_ROLE);
                List<KubernetesPod> pods = api.listPods(selector);
                for (KubernetesPod pod : pods) {
                    if (pod.isRunning()) {
                        state = ApplicationStatus.RUNNING;
                        break;
                    }
                }
            }
            return state;
        } catch (ApiException e) {
            if (e.getCode() != 404) {
                throw e;
            }
            return ApplicationStatus.UNKNOWN;
        }
    }

    /**
     * Deletes the owner and dependent resources; callers may retry after a partial API failure.
     *
     * @throws Exception if any resource could not be deleted
     */
    public void cancel() throws Exception {
        api.deleteApplication(applicationId);
        canceled = true;
    }

    /**
     * The deployer owns the shared SDK connection; closing a handle never cancels its application.
     */
    @Override
    public void close() {}
}
