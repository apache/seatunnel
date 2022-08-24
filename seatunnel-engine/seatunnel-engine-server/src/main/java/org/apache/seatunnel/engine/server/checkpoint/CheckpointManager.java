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

package org.apache.seatunnel.engine.server.checkpoint;

import java.util.HashMap;
import java.util.Map;

/**
 * Used to manage all checkpoints for a job.
 * <p>
 * Maintain the life cycle of the {@link CheckpointCoordinator} through the {@link CheckpointPlan} and the status of the job.
 * </p>
 */
public class CheckpointManager {

    private final Long jobId;

    /**
     * key: the pipeline id of the job;
     * <br> value: the checkpoint plan of the pipeline;
     */
    private final Map<Long, CheckpointPlan> checkpointPlanMap;

    /**
     * key: the pipeline id of the job;
     * <br> value: the checkpoint coordinator of the pipeline;
     */
    private final Map<Long, CheckpointCoordinator> coordinatorMap;

    private final CheckpointCoordinatorConfiguration config;

    public CheckpointManager(long jobId,
                             Map<Long, CheckpointPlan> checkpointPlanMap,
                             CheckpointCoordinatorConfiguration config) {
        this.jobId = jobId;
        this.checkpointPlanMap = checkpointPlanMap;
        this.config = config;
        this.coordinatorMap = new HashMap<>(checkpointPlanMap.size());
    }

    public CheckpointCoordinator getCheckpointCoordinator(long pipelineId) {
        return coordinatorMap.get(pipelineId);
    }
}
