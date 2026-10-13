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

package org.apache.seatunnel.resource.kubernetes.kubeclient.decorators;

import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;

import io.kubernetes.client.openapi.models.V1Pod;

import java.util.List;

/**
 * One composable feature of an application deployment.
 *
 * <p>A decorator may create or transform the owner Job with its master Pod, transform a worker Pod,
 * contribute accompanying Kubernetes resources that must exist next to the Job, or combine these.
 * The assembling caller applies decorators in list order. Accompanying resources are created after
 * the owner Job and always receive its server-assigned owner reference, so deleting the Job removes
 * exactly the resources created for this deployment.
 *
 * <p>The caller composes one ordered list per role and reuses shared decorators in both: a
 * decorator never branches on the Pod role, it is simply either part of a list or not. The list
 * applied to the owner Job also carries the features that contribute accompanying resources.
 */
public interface KubernetesStepDecorator {

    /**
     * Applies this feature to the owner Job, including its master Pod.
     *
     * <p>The first step of the list creates the Job; a feature step that needs the Job rejects a
     * {@code null} input.
     *
     * @param job owner Job, or {@code null} for the step that creates it
     * @return the Job containing this feature's changes
     */
    KubernetesJob decorateJob(KubernetesJob job);

    /**
     * Applies this feature to one Pod, including its identity and specification.
     *
     * @param pod Pod being assembled
     * @return the Pod containing this feature's changes
     */
    V1Pod decoratePod(V1Pod pod);

    /**
     * Builds resources that support this feature and must exist next to the owner Job.
     *
     * <p>The caller that assembles the decorator list appends the returned resources to the
     * application resources, in creation order.
     *
     * @return accompanying resources without owner references
     */
    List<KubernetesResource<?>> buildAccompanyingResources();
}
