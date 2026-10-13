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

import java.io.IOException;
import java.util.List;

/**
 * One composable feature of an application deployment.
 *
 * <p>A decorator may transform the owner Job's pod template, contribute accompanying Kubernetes
 * resources that must exist next to the Job, or both. Decorators are applied in list order.
 * Accompanying resources are created after the owner Job and always receive its server-assigned
 * owner reference, so deleting the Job removes exactly the resources created for this application.
 */
public interface KubernetesStepDecorator {

    /**
     * Applies this feature's pod changes to the owner Job.
     *
     * @param job owner Job being built
     * @return the Job containing this feature's changes
     */
    KubernetesJob decorateJob(KubernetesJob job);

    /**
     * Builds resources that support this feature and must exist next to the owner Job.
     *
     * @return accompanying resources without owner references
     * @throws IOException if a resource content cannot be serialized
     */
    List<KubernetesResource<?>> buildAccompanyingResources() throws IOException;
}
