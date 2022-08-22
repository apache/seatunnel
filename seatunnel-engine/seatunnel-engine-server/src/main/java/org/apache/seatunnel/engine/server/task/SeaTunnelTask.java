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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.engine.core.dag.actions.PartitionTransformAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.core.dag.actions.UnknownActionException;
import org.apache.seatunnel.engine.server.dag.physical.config.IntermediateQueueConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.SinkConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.SourceConfig;
import org.apache.seatunnel.engine.server.dag.physical.flow.Flow;
import org.apache.seatunnel.engine.server.dag.physical.flow.IntermediateExecutionFlow;
import org.apache.seatunnel.engine.server.dag.physical.flow.PhysicalExecutionFlow;
import org.apache.seatunnel.engine.server.dag.physical.flow.UnknownFlowException;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.flow.FlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.IntermediateQueueFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.OneInputFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.PartitionTransformSinkFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.PartitionTransformSourceFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.SinkFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.SourceFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.TransformFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.group.TaskGroupWithIntermediateQueue;

import lombok.NonNull;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public abstract class SeaTunnelTask extends AbstractTask {

    private static final long serialVersionUID = 2604309561613784425L;
    private final Flow executionFlow;

    protected FlowLifeCycle startFlowLifeCycle;

    private List<FlowLifeCycle> allCycles;

    protected List<OneInputFlowLifeCycle<Record<?>>> outputs;

    protected List<CompletableFuture<Void>> flowFutures;

    protected int indexID;

    private TaskGroup taskBelongGroup;

    public SeaTunnelTask(long jobID, TaskLocation taskID, int indexID, Flow executionFlow) {
        super(jobID, taskID);
        this.indexID = indexID;
        this.executionFlow = executionFlow;
    }

    @Override
    public void init() throws Exception {
        super.init();
        flowFutures = new ArrayList<>();
        allCycles = new ArrayList<>();
        startFlowLifeCycle = convertFlowToActionLifeCycle(executionFlow);
        for (FlowLifeCycle cycle : allCycles) {
            cycle.init();
        }
    }

    public void setTaskGroup(TaskGroup group) {
        this.taskBelongGroup = group;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private FlowLifeCycle convertFlowToActionLifeCycle(@NonNull Flow flow) throws Exception {

        FlowLifeCycle lifeCycle;
        List<OneInputFlowLifeCycle<Record<?>>> flowLifeCycles = new ArrayList<>();
        if (!flow.getNext().isEmpty()) {
            for (Flow f : executionFlow.getNext()) {
                flowLifeCycles.add((OneInputFlowLifeCycle<Record<?>>) convertFlowToActionLifeCycle(f));
            }
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        flowFutures.add(completableFuture);
        if (flow instanceof PhysicalExecutionFlow) {
            PhysicalExecutionFlow f = (PhysicalExecutionFlow) flow;
            if (f.getAction() instanceof SourceAction) {
                lifeCycle = createSourceFlowLifeCycle((SourceAction<?, ?, ?>) f.getAction(),
                        (SourceConfig) f.getConfig(), completableFuture);
                outputs = flowLifeCycles;
            } else if (f.getAction() instanceof SinkAction) {
                lifeCycle = new SinkFlowLifeCycle<>((SinkAction) f.getAction(), taskID, indexID, this,
                        ((SinkConfig) f.getConfig()).getCommitterTask(),
                        ((SinkConfig) f.getConfig()).isContainCommitter(), completableFuture);
            } else if (f.getAction() instanceof TransformChainAction) {
                lifeCycle =
                        new TransformFlowLifeCycle<SeaTunnelRow>(((TransformChainAction) f.getAction()).getTransforms(),
                                new SeaTunnelTransformCollector(flowLifeCycles), completableFuture);
            } else if (f.getAction() instanceof PartitionTransformAction) {
                // TODO use index and taskID to create ringbuffer list
                if (executionFlow.getNext().isEmpty()) {
                    lifeCycle = new PartitionTransformSinkFlowLifeCycle(completableFuture);
                } else {
                    lifeCycle = new PartitionTransformSourceFlowLifeCycle(completableFuture);
                }
            } else {
                throw new UnknownActionException(f.getAction());
            }
        } else if (flow instanceof IntermediateExecutionFlow) {
            IntermediateQueueConfig config =
                    ((IntermediateExecutionFlow<IntermediateQueueConfig>) flow).getConfig();
            lifeCycle = new IntermediateQueueFlowLifeCycle(completableFuture,
                    ((TaskGroupWithIntermediateQueue) taskBelongGroup)
                            .getBlockingQueueCache(config.getQueueID()));
            outputs = flowLifeCycles;
        } else {
            throw new UnknownFlowException(flow);
        }
        allCycles.add(lifeCycle);
        return lifeCycle;
    }

    protected abstract SourceFlowLifeCycle<?, ?> createSourceFlowLifeCycle(SourceAction<?, ?, ?> sourceAction,
                                                                           SourceConfig config,
                                                                           CompletableFuture<Void> completableFuture);

    protected void checkDone() {
        if (flowFutures.stream().allMatch(CompletableFuture::isDone)) {
            progress.done();
        }
    }

    @Override
    public void close() throws IOException {
        startFlowLifeCycle.close();
        progress.done();
    }

    @Override
    public Set<URL> getJarsUrl() {
        List<Flow> now = Collections.singletonList(executionFlow);
        Set<URL> urls = new HashSet<>();
        List<Flow> next = new ArrayList<>();
        while (!now.isEmpty()) {
            next.clear();
            now.forEach(n -> {
                if (n instanceof PhysicalExecutionFlow) {
                    urls.addAll(((PhysicalExecutionFlow) n).getAction().getJarUrls());
                }
                next.addAll(n.getNext());
            });
            now = next;
        }
        return urls;
    }
}
