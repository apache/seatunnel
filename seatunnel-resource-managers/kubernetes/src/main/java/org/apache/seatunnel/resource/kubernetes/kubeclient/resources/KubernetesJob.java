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

package org.apache.seatunnel.resource.kubernetes.kubeclient.resources;

import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobCondition;
import io.kubernetes.client.openapi.models.V1JobStatus;

/** Application owner Job and its durable lifecycle state. */
public final class KubernetesJob extends KubernetesResource<V1Job> {
    private static final String TRUE = "True";
    private static final String COMPLETE = "Complete";
    private static final String FAILED = "Failed";

    /**
     * Wraps one Kubernetes Job model.
     *
     * @param job Job returned by or intended for the Kubernetes API
     */
    public KubernetesJob(V1Job job) {
        super(job.getMetadata().getName(), job);
    }

    /** @return server-assigned UID used by dependent resource owner references */
    public String getUid() {
        return getInternalResource().getMetadata().getUid();
    }

    /** @return whether the Job controller currently reports an active master pod */
    public boolean isActive() {
        V1JobStatus status = getInternalResource().getStatus();
        return status != null && status.getActive() != null && status.getActive() > 0;
    }

    /** @return whether the Job has reached its successful terminal condition */
    public boolean isComplete() {
        return hasCondition(COMPLETE);
    }

    /** @return whether the Job has reached its failed terminal condition */
    public boolean isFailed() {
        return hasCondition(FAILED);
    }

    /** @return failure reason reported by Kubernetes, or {@code null} when unavailable */
    public String getFailureReason() {
        V1JobCondition condition = condition(FAILED);
        return condition == null ? null : condition.getReason();
    }

    private boolean hasCondition(String type) {
        return condition(type) != null;
    }

    private V1JobCondition condition(String type) {
        V1JobStatus status = getInternalResource().getStatus();
        if (status == null || status.getConditions() == null) {
            return null;
        }
        for (V1JobCondition condition : status.getConditions()) {
            if (type.equals(condition.getType()) && TRUE.equals(condition.getStatus())) {
                return condition;
            }
        }
        return null;
    }
}
