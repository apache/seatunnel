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

package org.apache.seatunnel.engine.server.observability.cluster;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.Node;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Tracks lightweight cluster-topology observability state for operator-facing metrics.
 *
 * <p>This service intentionally keeps a SeaTunnel Engine view of recent topology changes instead of
 * exposing Hazelcast-native listener state directly.
 */
public class ClusterObservabilityService {

    private final Node node;

    private long memberJoinTotal;
    private long memberLeaveTotal;
    private long masterChangeTotal;
    private long lastMemberJoinTimestampMs;
    private long lastMemberLeaveTimestampMs;
    private long lastMasterChangeTimestampMs;
    private Address lastKnownMasterAddress;

    public ClusterObservabilityService(Node node) {
        this.node = node;
        this.lastKnownMasterAddress = currentMasterAddress();
    }

    public synchronized void recordMemberAdded() {
        memberJoinTotal++;
        lastMemberJoinTimestampMs = System.currentTimeMillis();
        refreshMasterState();
    }

    public synchronized void recordMemberRemoved() {
        memberLeaveTotal++;
        lastMemberLeaveTimestampMs = System.currentTimeMillis();
        refreshMasterState();
    }

    public synchronized ClusterObservabilitySnapshot snapshot() {
        refreshMasterState();
        return new ClusterObservabilitySnapshot(
                memberJoinTotal,
                memberLeaveTotal,
                masterChangeTotal,
                lastMemberJoinTimestampMs,
                lastMemberLeaveTimestampMs,
                lastMasterChangeTimestampMs);
    }

    private void refreshMasterState() {
        Address currentMasterAddress = currentMasterAddress();
        if (currentMasterAddress == null) {
            return;
        }
        if (lastKnownMasterAddress == null) {
            lastKnownMasterAddress = currentMasterAddress;
            return;
        }
        if (!lastKnownMasterAddress.equals(currentMasterAddress)) {
            masterChangeTotal++;
            lastMasterChangeTimestampMs = System.currentTimeMillis();
            lastKnownMasterAddress = currentMasterAddress;
        }
    }

    private Address currentMasterAddress() {
        return node.getMasterAddress();
    }

    @Getter
    @AllArgsConstructor
    public static class ClusterObservabilitySnapshot {
        private final long memberJoinTotal;
        private final long memberLeaveTotal;
        private final long masterChangeTotal;
        private final long lastMemberJoinTimestampMs;
        private final long lastMemberLeaveTimestampMs;
        private final long lastMasterChangeTimestampMs;
    }
}
