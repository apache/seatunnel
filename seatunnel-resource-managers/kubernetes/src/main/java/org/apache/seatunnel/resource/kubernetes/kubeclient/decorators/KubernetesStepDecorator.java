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

import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;

import java.util.List;

/**
 * One composable feature of an application deployment, generic over the resource it decorates.
 *
 * <p>The caller composes one ordered list per resource kind: the owner Job chain ({@code
 * KubernetesStepDecorator<KubernetesJob>}) and the Pod chain ({@code
 * KubernetesStepDecorator<V1Pod>}). The Pod chain is applied to the master template from inside the
 * Job chain and to every worker Pod, so a feature that changes a Pod is implemented once and master
 * and workers cannot drift apart.
 *
 * <p>The first step of a list creates its resource and rejects a non-{@code null} input; later
 * steps reject {@code null}. Accompanying resources are created after the owner Job and always
 * receive its server-assigned owner reference, so deleting the Job removes exactly the resources
 * created for this deployment.
 *
 * @param <T> resource this step decorates, for example {@code KubernetesJob} or {@code V1Pod}
 */
public interface KubernetesStepDecorator<T> {

    /**
     * Applies this feature to the resource being assembled.
     *
     * <p>The first step of the list creates the resource; a feature step that needs it rejects a
     * {@code null} input.
     *
     * @param resource resource being assembled, or {@code null} for the step that creates it
     * @return the resource containing this feature's changes
     */
    T decorate(T resource);

    /**
     * Builds resources that support this feature and must exist next to the owner Job.
     *
     * <p>The caller that assembles the decorator list appends the returned resources to the
     * application resources, in creation order.
     *
     * @return accompanying resources without owner references
     */
    List<KubernetesResource<?>> build();
}
