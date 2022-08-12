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

import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.common.utils.NonCompletableFuture;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskGroupDefaultImpl;

import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.net.URL;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * PhysicalVertex is responsible for the scheduling and execution of a single task parallel
 * Each {@link org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex} generates some PhysicalVertex.
 * And the number of PhysicalVertex equals the {@link ExecutionVertex#getParallelism()}.
 */
public class PhysicalVertex {

    private static final ILogger LOGGER = Logger.getLogger(PhysicalVertex.class);

    /**
     * the index of PhysicalVertex
     */
    private final int subTaskGroupIndex;

    private final String taskNameWithSubtaskAndPipeline;

    private final int parallelism;

    private final TaskGroup taskGroup;

    private final ExecutorService executorService;

    private final FlakeIdGenerator flakeIdGenerator;

    private final int pipelineIndex;

    private final int totalPipelineNum;

    private final Set<URL> pluginJarsUrls;

    /**
     * When PhysicalVertex status turn to end, complete this future. And then the waitForCompleteByPhysicalVertex
     * in {@link SubPlan} whenComplete method will be called.
     */
    private final CompletableFuture<TaskExecutionState> taskFuture;


    /**
     * This future only can completion by the task run in {@link com.hazelcast.spi.impl.executionservice.ExecutionService }
     */
    private NonCompletableFuture<TaskExecutionState> waitForCompleteByExecutionService;

    public PhysicalVertex(int subTaskGroupIndex,
                          @NonNull ExecutorService executorService,
                          int parallelism,
                          @NonNull TaskGroupDefaultImpl taskGroup,
                          @NonNull CompletableFuture<TaskExecutionState> taskFuture,
                          @NonNull FlakeIdGenerator flakeIdGenerator,
                          int pipelineIndex,
                          int totalPipelineNum,
                          Set<URL> pluginJarsUrls) {
        this.subTaskGroupIndex = subTaskGroupIndex;
        this.executorService = executorService;
        this.parallelism = parallelism;
        this.taskGroup = taskGroup;
        this.flakeIdGenerator = flakeIdGenerator;
        this.pipelineIndex = pipelineIndex;
        this.totalPipelineNum = totalPipelineNum;
        this.pluginJarsUrls = pluginJarsUrls;
        this.taskNameWithSubtaskAndPipeline =
            String.format(
                "task: [%s (%d/%d)], pipeline: [%d/%d]",
                taskGroup.getTaskGroupName(),
                subTaskGroupIndex + 1,
                parallelism,
                pipelineIndex,
                totalPipelineNum);
        this.taskFuture = taskFuture;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public void deploy() throws JobException {

        // TODO really submit job to ExecutionService and get a NonCompletableFuture<ExecutionState>
        long executionId = flakeIdGenerator.newId();
        CompletableFuture<TaskExecutionState> uCompletableFuture = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(5000);
                return new TaskExecutionState(executionId, ExecutionState.FINISHED, null);
            } catch (InterruptedException e) {
                return new TaskExecutionState(executionId, ExecutionState.FAILED, e);
            }
        }, executorService);

        waitForCompleteByExecutionService = new NonCompletableFuture<>(uCompletableFuture);
        waitForCompleteByExecutionService.whenComplete((v, t) -> {
            if (t != null) {
                // TODO t.getMessage() need be replace
                LOGGER.info(String.format("The Task %s Failed with Exception: %s",
                    this.taskNameWithSubtaskAndPipeline,
                    t.getMessage()));
                taskFuture.complete(new TaskExecutionState(executionId, ExecutionState.FAILED, t));
            } else {
                LOGGER.info(String.format("The Task %s end with state %s",
                    this.taskNameWithSubtaskAndPipeline,
                    v));
                taskFuture.complete(v);
            }
        });
    }
}
