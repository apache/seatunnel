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

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.dag.physical.config.SourceConfig;
import org.apache.seatunnel.engine.server.dag.physical.flow.Flow;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.flow.SourceFlowLifeCycle;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SourceSeaTunnelTask<T, SplitT extends SourceSplit> extends SeaTunnelTask {

    private static final ILogger LOGGER = Logger.getLogger(SourceSeaTunnelTask.class);

    private transient SeaTunnelSourceCollector<T> collector;

    private final Object checkpointLock = new Object();
    public SourceSeaTunnelTask(long jobID, TaskLocation taskID, int indexID, Flow executionFlow) {
        super(jobID, taskID, indexID, executionFlow);
    }

    @Override
    public void init() throws Exception {
        super.init();
        LOGGER.info("starting seatunnel source task, index " + indexID);
        if (!(startFlowLifeCycle instanceof SourceFlowLifeCycle)) {
            throw new TaskRuntimeException("SourceSeaTunnelTask only support SourceFlowLifeCycle, but get " + startFlowLifeCycle.getClass().getName());
        } else {
            this.collector = new SeaTunnelSourceCollector<>(checkpointLock, outputs);
            ((SourceFlowLifeCycle<T, SplitT>) startFlowLifeCycle).setCollector(collector);
        }
    }

    @Override
    protected SourceFlowLifeCycle<?, ?> createSourceFlowLifeCycle(SourceAction<?, ?, ?> sourceAction,
                                                                  SourceConfig config, CompletableFuture<Void> completableFuture) {
        return new SourceFlowLifeCycle<>(sourceAction, indexID, config.getEnumeratorTask(), this, taskID, completableFuture);
    }

    @NonNull
    @Override
    @SuppressWarnings("unchecked")
    public ProgressState call() throws Exception {
        ((SourceFlowLifeCycle<T, SplitT>) startFlowLifeCycle).collect();
        return progress.toState();
    }

    @Override
    public void close() throws IOException {
        startFlowLifeCycle.close();
        progress.done();
    }

    public void receivedSourceSplit(List<SplitT> splits) {
        ((SourceFlowLifeCycle<T, SplitT>) startFlowLifeCycle).receivedSplits(splits);
    }

    @Override
    public void triggerCheckpoint(CheckpointBarrier barrier) throws Exception {
        SourceFlowLifeCycle<T, SplitT> sourceFlow = (SourceFlowLifeCycle<T, SplitT>) startFlowLifeCycle;
        // Block the reader from adding data to the collector.
        synchronized (checkpointLock) {
            sourceFlow.snapshotState(barrier.getId());
            collector.sendRecordToNext(new Record<>(barrier));
        }

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        ((SourceFlowLifeCycle<T, SplitT>) startFlowLifeCycle).notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        ((SourceFlowLifeCycle<T, SplitT>) startFlowLifeCycle).notifyCheckpointAborted(checkpointId);
    }
}
