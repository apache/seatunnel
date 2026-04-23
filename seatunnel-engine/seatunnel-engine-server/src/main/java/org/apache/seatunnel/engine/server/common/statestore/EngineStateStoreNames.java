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

package org.apache.seatunnel.engine.server.common.statestore;

import lombok.Getter;

/**
 * Bundle of map names required to build a Hazelcast-based {@link EngineStateStores} implementation.
 */
@Getter
public class EngineStateStoreNames {
    private EngineStateStoreNames() {}

    public static final String RUNNING_JOB_INFO = "engine_runningJobInfo";
    public static final String RUNNING_JOB_STATE = "engine_runningJobState";
    public static final String FINISHED_JOB_STATE = "engine_finishedJobState";
    public static final String FINISHED_JOB_METRICS = "engine_finishedJobMetrics";
    public static final String FINISHED_JOB_VERTEX_INFO = "engine_finishedJobVertexInfo";
    public static final String STATE_TIMESTAMPS = "engine_stateTimestamps";
    public static final String OWNED_SLOT_PROFILES = "engine_ownedSlotProfilesIMap";
    public static final String CHECKPOINT_ID = "engine_checkpoint-id-map";
    public static final String RUNNING_JOB_METRICS = "engine_runningJobMetrics";
    public static final String PENDING_PIPELINE_CLEANUP = "engine_pendingPipelineCleanup";
    public static final String CHECKPOINT_MONITOR = "engine_checkpoint_monitor";
    public static final String CONNECTOR_JAR_REF_COUNTERS = "engine_connectorJarRefCounters";
}
