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

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.dag.physical.config.SourceConfig;
import org.apache.seatunnel.engine.server.dag.physical.flow.Flow;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.flow.OneOutputFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.SourceFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

public class TransformSeaTunnelTask extends SeaTunnelTask {

    private static final ILogger LOGGER = Logger.getLogger(TransformSeaTunnelTask.class);

    public TransformSeaTunnelTask(
            long jobID, TaskLocation taskID, int indexID, Flow executionFlow) {
        super(jobID, taskID, indexID, executionFlow);
    }

    private Collector<Record<?>> collector;

    @Override
    public void init() throws Exception {
        super.init();
        LOGGER.info("starting seatunnel transform task, index " + indexID);
        collector = new SeaTunnelTransformCollector(outputs);
        if (!(startFlowLifeCycle instanceof OneOutputFlowLifeCycle)) {
            throw new TaskRuntimeException(
                    "TransformSeaTunnelTask only support OneOutputFlowLifeCycle, but get "
                            + startFlowLifeCycle.getClass().getName());
        }
    }

    @Override
    protected SourceFlowLifeCycle<?, ?> createSourceFlowLifeCycle(
            SourceAction<?, ?, ?> sourceAction,
            SourceConfig config,
            CompletableFuture<Void> completableFuture,
            MetricsContext metricsContext) {
        throw new UnsupportedOperationException(
                "TransformSeaTunnelTask can't create SourceFlowLifeCycle");
    }

    @Override
    protected void collect() throws Exception {
        ((OneOutputFlowLifeCycle<Record<?>>) startFlowLifeCycle).collect(collector);
    }

    @NonNull @Override
    public ProgressState call() throws Exception {
        stateProcess();
        return progress.toState();
    }

    @Override
    public void triggerBarrier(Barrier checkpointBarrier) throws Exception {
        // nothing
    }
}
