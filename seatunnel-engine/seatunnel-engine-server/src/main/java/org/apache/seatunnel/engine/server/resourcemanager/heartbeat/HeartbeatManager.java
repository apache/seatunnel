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

package org.apache.seatunnel.engine.server.resourcemanager.heartbeat;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HeartbeatManager {

    private static final ILogger LOGGER = Logger.getLogger(HeartbeatManager.class);

    private static final int DEFAULT_TIMEOUT_MILLISECONDS = 5000;
    private static final int DEFAULT_INTERVAL_MILLISECONDS = 3000;
    private final Map<String, Long> lastHeartbeat;
    private final HeartbeatListener listener;

    public void heartbeat(String nodeID) {
        lastHeartbeat.put(nodeID, System.currentTimeMillis());
    }

    public void removeNode(String nodeID) {
        lastHeartbeat.remove(nodeID);
    }

    public HeartbeatManager(HeartbeatListener listener) {
        this.listener = listener;
        this.lastHeartbeat = new ConcurrentHashMap<>();
    }

    public void start(ScheduledExecutorService scheduledExecutorService) {
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            lastHeartbeat.forEach((nodeID, last) -> {
                    long now = System.currentTimeMillis();
                    // TODO support custom timeout
                    if (now - last > DEFAULT_TIMEOUT_MILLISECONDS) {
                        LOGGER.severe("Node heartbeat timeout, disconnected for heartbeatManager. " +
                                "NodeID: " + nodeID);
                        listener.nodeDisconnected(nodeID);
                    }
                }
            );
        }, 0, DEFAULT_INTERVAL_MILLISECONDS, TimeUnit.MILLISECONDS);
    }
}
