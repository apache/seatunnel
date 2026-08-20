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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.resourcemanager.autoscaler.AutoscalerRecommendation;
import org.apache.seatunnel.engine.server.resourcemanager.autoscaler.WorkerAutoscaler;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

/**
 * REST service for exposing autoscaler state and recommendations.
 *
 * <p>This service is only available on the master node. When queried from a non-master node, it
 * returns a default response indicating the autoscaler is not available.
 */
public class AutoscalerService extends BaseService {

    private final NodeEngineImpl nodeEngine;

    public AutoscalerService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        this.nodeEngine = nodeEngine;
    }

    /**
     * Returns the current autoscaler state and recommendation as a JSON object.
     *
     * @return JSON object with autoscaler information
     */
    public JsonObject getAutoscalerInfo() {
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);
        if (seaTunnelServer == null || !seaTunnelServer.isMasterNode()) {
            return defaultResponse();
        }

        CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();
        if (coordinatorService == null) {
            return defaultResponse();
        }

        WorkerAutoscaler autoscaler = coordinatorService.getWorkerAutoscaler();
        if (autoscaler == null) {
            JsonObject obj = new JsonObject();
            obj.add("enabled", false);
            obj.add("message", "Autoscaler is not enabled on this cluster.");
            return obj;
        }

        AutoscalerRecommendation recommendation = autoscaler.getCurrentRecommendation();
        WorkerAutoscaler.AutoscalerState state = autoscaler.getAutoscalerState();

        JsonObject obj = new JsonObject();
        obj.add("enabled", true);

        // State
        JsonObject stateObj = new JsonObject();
        stateObj.add("currentWorkerCount", state.getCurrentWorkerCount());
        stateObj.add("totalSlots", state.getTotalSlots());
        stateObj.add("assignedSlots", state.getAssignedSlots());
        stateObj.add("slotUsageRatio", state.getSlotUsageRatio());
        stateObj.add("averageCpuLoad", state.getAverageCpuLoad());
        stateObj.add("averageMemoryLoad", state.getAverageMemoryLoad());
        stateObj.add("minWorkers", state.getMinWorkers());
        stateObj.add("maxWorkers", state.getMaxWorkers());
        obj.add("state", stateObj);

        // Recommendation
        JsonObject recObj = new JsonObject();
        recObj.add("action", recommendation.getAction().name());
        recObj.add("reason", recommendation.getReason());
        recObj.add("currentWorkerCount", recommendation.getCurrentWorkerCount());
        recObj.add("targetWorkerCount", recommendation.getTargetWorkerCount());
        recObj.add("recommendationOnly", recommendation.isRecommendationOnly());
        recObj.add("slotUsageRatio", recommendation.getSlotUsageRatio());
        recObj.add("averageCpuLoad", recommendation.getAverageCpuLoad());
        recObj.add("averageMemoryLoad", recommendation.getAverageMemoryLoad());
        if (recommendation.getTimestamp() != null) {
            recObj.add("timestamp", recommendation.getTimestamp().toString());
        }
        obj.add("recommendation", recObj);

        return obj;
    }

    private JsonObject defaultResponse() {
        JsonObject obj = new JsonObject();
        obj.add("enabled", false);
        obj.add("message", "Autoscaler is only available on the master node.");
        return obj;
    }
}
