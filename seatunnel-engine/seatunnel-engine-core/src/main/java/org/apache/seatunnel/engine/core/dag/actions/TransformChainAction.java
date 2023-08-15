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

package org.apache.seatunnel.engine.core.dag.actions;

import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;

import lombok.NonNull;

import java.net.URL;
import java.util.List;
import java.util.Set;

public class TransformChainAction<T> extends AbstractAction {

    private static final long serialVersionUID = -340174711145367535L;
    private final List<SeaTunnelTransform<T>> transforms;

    public TransformChainAction(
            long id,
            @NonNull String name,
            @NonNull List<Action> upstreams,
            @NonNull Set<URL> jarUrls,
            @NonNull Set<ConnectorJarIdentifier> connectorJarIdentifiers,
            @NonNull List<SeaTunnelTransform<T>> transforms) {
        super(id, name, upstreams, jarUrls, connectorJarIdentifiers);
        this.transforms = transforms;
    }

    public TransformChainAction(
            long id,
            @NonNull String name,
            @NonNull Set<URL> jarUrls,
            @NonNull Set<ConnectorJarIdentifier> connectorJarIdentifiers,
            @NonNull List<SeaTunnelTransform<T>> transforms) {
        super(id, name, jarUrls, connectorJarIdentifiers);
        this.transforms = transforms;
    }

    public List<SeaTunnelTransform<T>> getTransforms() {
        return transforms;
    }
}
