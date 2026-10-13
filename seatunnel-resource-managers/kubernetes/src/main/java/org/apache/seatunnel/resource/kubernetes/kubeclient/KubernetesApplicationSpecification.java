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

package org.apache.seatunnel.resource.kubernetes.kubeclient;

import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Complete set of Kubernetes components that represents one application before creation.
 *
 * <p>The owner Job is created first; every accompanying resource then receives the Job's owner
 * reference before it is created. No model in this composition has been sent to the API server.
 */
public final class KubernetesApplicationSpecification {
    private final KubernetesJob applicationJob;
    private final List<KubernetesResource<?>> accompanyingResources;

    public KubernetesApplicationSpecification(
            KubernetesJob applicationJob, List<KubernetesResource<?>> accompanyingResources) {
        this.applicationJob = Objects.requireNonNull(applicationJob, "applicationJob");
        this.accompanyingResources =
                Collections.unmodifiableList(
                        new ArrayList<>(Objects.requireNonNull(accompanyingResources)));
    }

    /** @return suspended owner Job to create first */
    public KubernetesJob getApplicationJob() {
        return applicationJob;
    }

    /** @return resources owned by the created Job, in creation order */
    public List<KubernetesResource<?>> getAccompanyingResources() {
        return accompanyingResources;
    }
}
