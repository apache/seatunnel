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

import io.kubernetes.client.openapi.models.V1Pod;

import java.util.List;

/**
 * One composable feature of an application deployment.
 *
 * <p>A step decorates one Pod and optionally contributes the Kubernetes resources that support the
 * feature. The caller creates the Pod skeleton first - the master Pod that is later wrapped into
 * the owner Job, or a worker Pod created by the application master - and then applies one ordered
 * list of steps. Master and worker Pods share the same steps, so a feature that changes a Pod is
 * implemented once and the two roles cannot drift apart.
 *
 * <p>Accompanying resources are collected only while the application is assembled; they are created
 * after the owner Job and always receive its server-assigned owner reference, so deleting the Job
 * removes exactly the resources created for this deployment.
 */
public interface KubernetesStepDecorator {

    /**
     * Applies this feature to the Pod being assembled.
     *
     * @param pod Pod created by the caller, never {@code null}
     * @return the Pod containing this feature's changes
     */
    V1Pod decorate(V1Pod pod);

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
