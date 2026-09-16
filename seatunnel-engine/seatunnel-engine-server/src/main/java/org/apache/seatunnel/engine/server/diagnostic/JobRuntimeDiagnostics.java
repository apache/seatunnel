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

package org.apache.seatunnel.engine.server.diagnostic;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

/**
 * Builds the {@code diagnostics} block of the job-info REST response.
 *
 * <p>Only signals the engine already tracks are exposed: the per-state entry timestamps kept in
 * {@link Constant#IMAP_STATE_TIMESTAMPS} (job level and pipeline level) and the pipeline restore
 * counter kept in {@link SubPlan}. Nothing here is recorded on the state transition path, so this
 * is a pure read.
 *
 * <p>The pipeline part is only available where the {@link JobMaster} lives, therefore this must be
 * called on the master node (REST callers on other members go through {@code
 * GetJobDiagnosticsOperation}).
 */
@Slf4j
public final class JobRuntimeDiagnostics {

    public static final String JOB_ID = "jobId";
    public static final String GENERATED_AT = "generatedAt";
    public static final String STATE_TIMESTAMPS = "stateTimestamps";
    public static final String PIPELINES = "pipelines";
    public static final String PIPELINE_ID = "pipelineId";
    public static final String PIPELINE_STATUS = "pipelineStatus";
    public static final String RESTORE_COUNT = "restoreCount";
    public static final String MAX_RESTORE_COUNT = "maxRestoreCount";
    public static final String TOTAL_PIPELINE_RESTORE_COUNT = "totalPipelineRestoreCount";

    private JobRuntimeDiagnostics() {}

    /** Collects the diagnostics of one job from the state timestamps map and the physical plan. */
    public static JsonObject build(SeaTunnelServer server, long jobId) {
        JsonObject root = new JsonObject();
        root.add(JOB_ID, String.valueOf(jobId));
        root.add(GENERATED_AT, System.currentTimeMillis());

        IMap<Object, Long[]> stateTimestampsMap =
                server.getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_STATE_TIMESTAMPS);
        root.add(
                STATE_TIMESTAMPS,
                toTimestampsJson(stateTimestampsMap.get(jobId), JobStatus.values()));

        JsonArray pipelines = new JsonArray();
        root.add(PIPELINES, pipelines);

        int totalRestoreCount = 0;
        for (SubPlan subPlan : pipelineList(server, jobId)) {
            PipelineStatus pipelineStatus = subPlan.getPipelineState();
            int restoreCount = subPlan.getPipelineRestoreNum();
            totalRestoreCount += restoreCount;
            pipelines.add(
                    new JsonObject()
                            .add(PIPELINE_ID, subPlan.getPipelineId())
                            .add(
                                    PIPELINE_STATUS,
                                    pipelineStatus == null ? null : pipelineStatus.toString())
                            .add(RESTORE_COUNT, restoreCount)
                            .add(MAX_RESTORE_COUNT, subPlan.getPipelineMaxRestoreNum())
                            .add(
                                    STATE_TIMESTAMPS,
                                    toTimestampsJson(
                                            stateTimestampsMap.get(subPlan.getPipelineLocation()),
                                            PipelineStatus.values())));
        }
        root.add(TOTAL_PIPELINE_RESTORE_COUNT, totalRestoreCount);
        return root;
    }

    /**
     * Returns the pipelines of a running job, or an empty list when the job is no longer
     * coordinated by this member (finished job, master switch in progress).
     */
    private static List<SubPlan> pipelineList(SeaTunnelServer server, long jobId) {
        try {
            JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);
            if (jobMaster == null) {
                return Collections.emptyList();
            }
            PhysicalPlan physicalPlan = jobMaster.getPhysicalPlan();
            if (physicalPlan == null) {
                return Collections.emptyList();
            }
            return physicalPlan.getPipelineList();
        } catch (Throwable t) {
            log.debug("Get pipeline diagnostics of job {} failed: {}", jobId, t.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Renders a {@code stateTimestamps} array as a state name to epoch millis object, skipping
     * states that were never entered.
     */
    private static JsonObject toTimestampsJson(Long[] stateTimestamps, Enum<?>[] states) {
        JsonObject json = new JsonObject();
        if (stateTimestamps == null) {
            return json;
        }
        for (Enum<?> state : states) {
            int ordinal = state.ordinal();
            if (ordinal < stateTimestamps.length && stateTimestamps[ordinal] != null) {
                json.add(state.name(), stateTimestamps[ordinal].longValue());
            }
        }
        return json;
    }
}
