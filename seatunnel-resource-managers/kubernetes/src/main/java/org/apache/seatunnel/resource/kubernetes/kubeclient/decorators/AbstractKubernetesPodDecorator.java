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

import io.kubernetes.client.openapi.models.V1Pod;

/**
 * Base for steps of the Pod decorator chain.
 *
 * <p>The first step of the chain creates the Pod and every later step receives it, so this class
 * rejects a {@code null} input with a clear message instead of failing later on the missing
 * specification. Steps that contribute accompanying resources override {@link #build()}.
 */
public abstract class AbstractKubernetesPodDecorator
        extends AbstractKubernetesStepDecorator<V1Pod> {

    @Override
    public final V1Pod decorate(V1Pod pod) {
        if (pod == null) {
            throw new IllegalStateException(
                    getClass().getSimpleName() + " must follow the step that creates the Pod");
        }
        return apply(pod);
    }

    /**
     * Applies this feature to a Pod that the chain has already created.
     *
     * @param pod Pod being assembled, never {@code null}
     * @return the Pod containing this feature's changes
     */
    protected abstract V1Pod apply(V1Pod pod);
}
