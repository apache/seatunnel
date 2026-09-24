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

import io.kubernetes.client.openapi.models.V1Pod;

/** Master or worker Pod with lifecycle helpers used by status monitoring. */
public final class KubernetesPod extends KubernetesResource<V1Pod> {
    private static final String RUNNING = "Running";
    private static final String FAILED = "Failed";
    private static final String SUCCEEDED = "Succeeded";

    /**
     * Wraps one Kubernetes Pod model.
     *
     * @param pod Pod returned by or intended for the Kubernetes API
     */
    public KubernetesPod(V1Pod pod) {
        super(pod.getMetadata().getName(), pod);
    }

    /** @return Kubernetes Pod phase, or {@code null} before a phase is assigned */
    public String getPhase() {
        return getInternalResource().getStatus() == null
                ? null
                : getInternalResource().getStatus().getPhase();
    }

    /** @return whether the Pod is currently in the Running phase */
    public boolean isRunning() {
        return RUNNING.equals(getPhase());
    }

    /** @return whether the Pod has reached a terminal phase */
    public boolean isTerminated() {
        String phase = getPhase();
        return FAILED.equals(phase) || SUCCEEDED.equals(phase);
    }

    /** @return whether Kubernetes has accepted deletion of this Pod */
    public boolean isTerminating() {
        return getInternalResource().getMetadata().getDeletionTimestamp() != null;
    }
}
