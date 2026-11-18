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

package org.apache.seatunnel.resource.yarn;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceID;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceIDRetrievable;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnResource;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

/** Allocated YARN worker container with stable identity used by the resource-manager driver. */
public final class YarnWorkerNode extends YarnResource<Container> implements ResourceIDRetrievable {
    private final ResourceID resourceID;

    public YarnWorkerNode(Container container, ResourceID resourceID) {
        super(container.getId().toString(), container);
        this.resourceID = Preconditions.checkNotNull(resourceID);
    }

    /** @return worker identifier exposed to the Zeta resource manager */
    public String getWorkerId() {
        return getName();
    }

    /** @return native container descriptor used only for NodeManager launch */
    public Container getContainer() {
        return getInternalResource();
    }

    /** @return native container identifier used for stop and release operations */
    public ContainerId getContainerId() {
        return getInternalResource().getId();
    }

    /** @return NodeManager identity hosting this worker */
    public NodeId getNodeId() {
        return getInternalResource().getNodeId();
    }

    @Override
    public ResourceID getResourceID() {
        return resourceID;
    }
}
