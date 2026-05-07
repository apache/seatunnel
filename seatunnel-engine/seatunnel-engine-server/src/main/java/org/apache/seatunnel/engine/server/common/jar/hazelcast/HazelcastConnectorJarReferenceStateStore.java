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

package org.apache.seatunnel.engine.server.common.jar.hazelcast;

import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.RefCount;
import org.apache.seatunnel.engine.server.common.jar.ConnectorJarReferenceStateStore;

import com.hazelcast.map.IMap;

import java.util.Objects;

public class HazelcastConnectorJarReferenceStateStore implements ConnectorJarReferenceStateStore {

    private final IMap<ConnectorJarIdentifier, RefCount> connectorJarRefCounters;

    public HazelcastConnectorJarReferenceStateStore(
            IMap<ConnectorJarIdentifier, RefCount> connectorJarRefCounters) {
        this.connectorJarRefCounters =
                Objects.requireNonNull(connectorJarRefCounters, "connectorJarRefCounters");
    }

    @Override
    public boolean containsKey(ConnectorJarIdentifier connectorJarIdentifier) {
        return connectorJarRefCounters.containsKey(connectorJarIdentifier);
    }

    @Override
    public void remove(ConnectorJarIdentifier connectorJarIdentifier) {
        connectorJarRefCounters.remove(connectorJarIdentifier);
    }

    @Override
    public RefCount putIfAbsent(ConnectorJarIdentifier connectorJarIdentifier, RefCount value) {
        return connectorJarRefCounters.putIfAbsent(connectorJarIdentifier, value);
    }

    @Override
    public void put(ConnectorJarIdentifier connectorJarIdentifier, RefCount value) {
        connectorJarRefCounters.put(connectorJarIdentifier, value);
    }

    @Override
    public RefCount get(ConnectorJarIdentifier connectorJarIdentifier) {
        return connectorJarRefCounters.get(connectorJarIdentifier);
    }

    @Override
    public void increaseReference(ConnectorJarIdentifier connectorJarIdentifier) {
        connectorJarRefCounters.compute(
                connectorJarIdentifier,
                (key, refCount) -> {
                    if (refCount != null) {
                        refCount.setReferences(refCount.getReferences() + 1);
                    }
                    return refCount;
                });
    }

    @Override
    public void decreaseReference(ConnectorJarIdentifier connectorJarIdentifier) {
        connectorJarRefCounters.compute(
                connectorJarIdentifier,
                (key, refCount) -> {
                    if (refCount != null) {
                        refCount.setReferences(refCount.getReferences() - 1);
                    }
                    return refCount;
                });
    }
}
