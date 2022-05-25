/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.executionplan;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.engine.api.source.Boundedness;
import org.apache.seatunnel.engine.config.Configuration;
import org.apache.seatunnel.engine.logicalplan.LogicalPlan;

import java.util.concurrent.ScheduledExecutorService;

public class ExecutionPlanFactory {

    private Configuration configuration;

    private final ScheduledExecutorService executorService;

    public ExecutionPlanFactory(Configuration configuration, ScheduledExecutorService executorService) {
        this.configuration = configuration;
        this.executorService = executorService;
    }

    public BaseExecutionPlan createExecutionPlan(LogicalPlan logicalPlan, long initializationTimestamp) {

        checkNotNull(logicalPlan);
        checkArgument(logicalPlan.getMaxParallelism() > 0, "maxParallelism must greater than 0");

        int sourceParallelism = logicalPlan.getMaxParallelism();

        // sometimes user may set max parallelism exceeded the source split num. we will use the smaller of unassignedSplitNum and parallelism in bounded job
        if (logicalPlan.getJobInformation().getBoundedness().equals(Boundedness.BOUNDED)) {
            // get unassigned split number from source
            int unassignedSplitNum = logicalPlan.getSource().getTotalTaskNumber();
            sourceParallelism = Math.min(unassignedSplitNum, logicalPlan.getMaxParallelism());
        }
        BaseExecutionPlan batchExecutionPlan = new BaseExecutionPlan(this.executorService, configuration, logicalPlan.getJobInformation(), initializationTimestamp, sourceParallelism);
        batchExecutionPlan.initExecutionTasks(logicalPlan.getLogicalTasks());

        return batchExecutionPlan;
    }
}
