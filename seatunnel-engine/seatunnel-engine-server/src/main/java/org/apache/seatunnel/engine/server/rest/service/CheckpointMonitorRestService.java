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

import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointInfo;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.core.checkpoint.InProgressCheckpoint;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CheckpointMonitorRestService extends BaseService {

    public CheckpointMonitorRestService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public JsonObject getOverview(long jobId) {
        CheckpointMonitorService monitorService = getMonitorService();
        JsonObject result = new JsonObject().add("jobId", String.valueOf(jobId));
        if (monitorService == null) {
            return result;
        }
        Optional<CheckpointOverview> overview = monitorService.getOverview(jobId);
        overview.ifPresent(
                snapshot -> {
                    result.add("updatedAt", snapshot.getUpdatedAt());
                    JsonArray pipelines = new JsonArray();
                    for (Map.Entry<Integer, PipelineCheckpointOverview> entry :
                            snapshot.getPipelines().entrySet()) {
                        pipelines.add(pipelineOverviewToJson(entry.getKey(), entry.getValue()));
                    }
                    result.add("pipelines", pipelines);
                });
        return result;
    }

    public JsonArray getHistory(
            long jobId, Integer pipelineId, int limit, CheckpointStatus status) {
        CheckpointMonitorService monitorService = getMonitorService();
        JsonArray result = new JsonArray();
        if (monitorService == null) {
            return result;
        }
        List<CheckpointHistoryEntry> entries =
                monitorService.getHistory(jobId, pipelineId, limit, status);
        entries.forEach(
                entry ->
                        result.add(
                                checkpointHistoryToJson(
                                        entry.getPipelineId(), entry.getCheckpointInfo())));
        return result;
    }

    private CheckpointMonitorService getMonitorService() {
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);
        if (seaTunnelServer == null) {
            seaTunnelServer = getSeaTunnelServer(false);
        }
        return seaTunnelServer.getCheckpointMonitorService();
    }

    private JsonObject pipelineOverviewToJson(int pipelineId, PipelineCheckpointOverview overview) {
        JsonObject object = new JsonObject().add("pipelineId", pipelineId);
        JsonObject counts = new JsonObject();
        counts.add("triggered", overview.getCounts().getTriggered());
        counts.add("completed", overview.getCounts().getCompleted());
        counts.add("failed", overview.getCounts().getFailed());
        counts.add("inProgress", overview.getCounts().getInProgress());
        counts.add("restored", overview.getCounts().getRestored());
        object.add("counts", counts);
        object.add("latestCompleted", checkpointInfoToJson(overview.getLatestCompleted()));
        object.add("latestFailed", checkpointInfoToJson(overview.getLatestFailed()));
        object.add("latestSavepoint", checkpointInfoToJson(overview.getLatestSavepoint()));

        JsonArray inProgress = new JsonArray();
        for (InProgressCheckpoint checkpoint : overview.getInProgress()) {
            JsonObject cp =
                    new JsonObject()
                            .add("checkpointId", checkpoint.getCheckpointId())
                            .add(
                                    "checkpointType",
                                    checkpoint.getCheckpointType() == null
                                            ? null
                                            : checkpoint.getCheckpointType().getName())
                            .add("triggerTimestamp", checkpoint.getTriggerTimestamp())
                            .add("acknowledged", checkpoint.getAcknowledgedSubtasks())
                            .add("total", checkpoint.getTotalSubtasks());
            inProgress.add(cp);
        }
        object.add("inProgress", inProgress);

        JsonArray history = new JsonArray();
        overview.getHistory()
                .forEach(
                        entry ->
                                history.add(
                                        checkpointHistoryToJson(
                                                pipelineId, entry.getCheckpointInfo())));
        object.add("history", history);
        return object;
    }

    private JsonObject checkpointHistoryToJson(int pipelineId, CheckpointInfo info) {
        JsonObject obj = new JsonObject().add("pipelineId", pipelineId);
        obj.add("checkpoint", checkpointInfoToJson(info));
        return obj;
    }

    private JsonObject checkpointInfoToJson(CheckpointInfo info) {
        if (info == null) {
            return new JsonObject();
        }
        JsonObject object = new JsonObject();
        object.add("checkpointId", info.getCheckpointId());
        object.add(
                "checkpointType",
                info.getCheckpointType() == null ? null : info.getCheckpointType().getName());
        object.add("status", info.getStatus() == null ? null : info.getStatus().name());
        object.add("triggerTimestamp", info.getTriggerTimestamp());
        if (info.getCompletedTimestamp() != null) {
            object.add("completedTimestamp", info.getCompletedTimestamp());
        }
        if (info.getDurationMillis() != null) {
            object.add("durationMillis", info.getDurationMillis());
        }
        object.add("stateSize", info.getStateSize());
        if (info.getFailureReason() != null) {
            object.add("failureReason", info.getFailureReason());
        }
        return object;
    }
}
