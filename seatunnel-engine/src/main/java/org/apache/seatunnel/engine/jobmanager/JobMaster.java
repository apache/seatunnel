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

package org.apache.seatunnel.engine.jobmanager;

import org.apache.seatunnel.engine.config.Configuration;
import org.apache.seatunnel.engine.execution.TaskExecution;
import org.apache.seatunnel.engine.executionplan.BaseExecutionPlan;
import org.apache.seatunnel.engine.executionplan.ExecutionPlanFactory;
import org.apache.seatunnel.engine.logicalplan.LogicalPlan;
import org.apache.seatunnel.engine.scheduler.SchedulerFactory;
import org.apache.seatunnel.engine.scheduler.SchedulerStrategy;
import org.apache.seatunnel.engine.task.TaskExecutionState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

public class JobMaster {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private JobMasterId jobMasterId;

    private LogicalPlan logicalPlan;

    private SchedulerStrategy schedulerStrategy;

    private ScheduledExecutorService scheduledExecutorService;

    private Configuration configuration;

    private TaskExecution taskExecution;

    private long initializationTimestamp;

    public JobMaster(JobMasterId jobMasterId,
                     Configuration configuration,
                     LogicalPlan logicalPlan,
                     ScheduledExecutorService scheduledExecutorService,
                     long initializationTimestamp) {
        this.jobMasterId = jobMasterId;
        this.configuration = configuration;
        this.logicalPlan = logicalPlan;
        this.scheduledExecutorService = scheduledExecutorService;
        this.taskExecution = taskExecution;
        this.initializationTimestamp = initializationTimestamp;

        schedulerStrategy = createScheduler();

    }

    private SchedulerStrategy createScheduler() {
        ExecutionPlanFactory executionPlanFactory = new ExecutionPlanFactory(configuration, scheduledExecutorService);
        BaseExecutionPlan executionPlan = executionPlanFactory.createExecutionPlan(logicalPlan, this.initializationTimestamp);

        SchedulerFactory schedulerFactory = new SchedulerFactory(executionPlan);
        SchedulerStrategy schedulerStrategy = schedulerFactory.createSchedulerStrategy();
        return schedulerStrategy;
    }

    public void run() {
        logger.info("begin run job '{}' ({}) in JobMaster id {}",
            logicalPlan.getJobInformation().getJobName(),
            logicalPlan.getJobInformation().getJobId(),
            jobMasterId);

        schedulerStrategy.startScheduling(taskExecution);

        //exit when all task run end
    }

    public void registerTaskExecution(TaskExecution taskExecution) {
        this.taskExecution = taskExecution;
    }

    public void updateExecutionState(TaskExecutionState state) {
        if (!schedulerStrategy.updateExecutionState(state)) {
            throw new RuntimeException("update execution state panic");
        }
    }
}
