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

package org.apache.seatunnel.engine.server.dag.physical;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.server.QueueType;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointPlan;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionPlanGenerator;
import org.apache.seatunnel.engine.server.storage.MapStorage;

import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.NonNull;

import java.util.Map;
import java.util.concurrent.ExecutorService;

public class PlanUtils {

    public static Tuple2<PhysicalPlan, Map<Integer, CheckpointPlan>> fromLogicalDAG(
            @NonNull LogicalDag logicalDag,
            @NonNull NodeEngine nodeEngine,
            @NonNull JobImmutableInformation jobImmutableInformation,
            long initializationTimestamp,
            @NonNull ExecutorService executorService,
            @NonNull ClassLoaderService classLoaderService,
            @NonNull FlakeIdGenerator flakeIdGenerator,
            @NonNull MapStorage runningJobStateMapStorage,
            @NonNull MapStorage runningJobStateTimestampsMapStorage,
            @NonNull QueueType queueType,
            @NonNull EngineConfig engineConfig) {
        return new PhysicalPlanGenerator(
                        new ExecutionPlanGenerator(
                                        logicalDag, jobImmutableInformation, engineConfig)
                                .generate(),
                        nodeEngine,
                        jobImmutableInformation,
                        initializationTimestamp,
                        executorService,
                        classLoaderService,
                        flakeIdGenerator,
                        runningJobStateMapStorage,
                        runningJobStateTimestampsMapStorage,
                        queueType)
                .generate();
    }
}
