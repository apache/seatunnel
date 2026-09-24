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

package org.apache.seatunnel.resource.yarn.cluster;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.Objects;

/** Allocated YARN worker container with stable identity used by the resource-manager driver. */
final class YarnWorkerNode {
    private final Container container;

    YarnWorkerNode(Container container) {
        this.container = Objects.requireNonNull(container, "container");
    }

    /** @return worker identifier exposed to the Zeta resource manager */
    String getWorkerId() {
        return container.getId().toString();
    }

    /** @return native container descriptor used only for NodeManager launch */
    Container getContainer() {
        return container;
    }

    /** @return native container identifier used for stop and release operations */
    ContainerId getContainerId() {
        return container.getId();
    }

    /** @return NodeManager identity hosting this worker */
    NodeId getNodeId() {
        return container.getNodeId();
    }
}
