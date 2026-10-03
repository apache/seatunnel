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

import java.util.Objects;

/**
 * Base type for a named Kubernetes resource backed by one client SDK model.
 *
 * <p>The wrapper keeps Kubernetes SDK types inside the kubeclient layer while exposing stable
 * resource semantics to application deployment and resource-management code.
 *
 * @param <T> Kubernetes client SDK model type
 */
public abstract class KubernetesResource<T> {
    private final String name;
    private final T internalResource;

    protected KubernetesResource(String name, T internalResource) {
        this.name = Objects.requireNonNull(name, "name");
        this.internalResource = Objects.requireNonNull(internalResource, "internalResource");
    }

    /** @return namespace-local Kubernetes resource name */
    public final String getName() {
        return name;
    }

    /**
     * Returns the SDK model used only by kubeclient implementations and resource factories.
     *
     * @return mutable Kubernetes client SDK model
     */
    public final T getInternalResource() {
        return internalResource;
    }
}
