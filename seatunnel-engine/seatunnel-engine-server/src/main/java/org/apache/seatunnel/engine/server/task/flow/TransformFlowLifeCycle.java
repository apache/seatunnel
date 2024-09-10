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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class TransformFlowLifeCycle<T> extends ActionFlowLifeCycle
        implements OneInputFlowLifeCycle<Record<?>> {

    private final TransformChainAction<T> action;

    private final List<SeaTunnelTransform<T>> transform;

    private final Collector<Record<?>> collector;

    public TransformFlowLifeCycle(
            TransformChainAction<T> action,
            SeaTunnelTask runningTask,
            Collector<Record<?>> collector,
            CompletableFuture<Void> completableFuture) {
        super(action, runningTask, completableFuture);
        this.action = action;
        this.transform = action.getTransforms();
        this.collector = collector;
    }

    @Override
    public void open() throws Exception {
        super.open();
        for (SeaTunnelTransform<T> t : transform) {
            try {
                t.open();
            } catch (Exception e) {
                log.error(
                        "Open transform: {} failed, cause: {}",
                        t.getPluginName(),
                        e.getMessage(),
                        e);
            }
        }
    }

    @Override
    public void received(Record<?> record) {
        if (record.getData() instanceof Barrier) {
            CheckpointBarrier barrier = (CheckpointBarrier) record.getData();
            if (barrier.prepareClose(this.runningTask.getTaskLocation())) {
                prepareClose = true;
            }
            if (barrier.snapshot()) {
                runningTask.addState(barrier, ActionStateKey.of(action), Collections.emptyList());
            }
            // ack after #addState
            runningTask.ack(barrier);
            collector.collect(record);
        } else if (record.getData() instanceof SchemaChangeEvent) {
            if (prepareClose) {
                return;
            }
            SchemaChangeEvent event = (SchemaChangeEvent) record.getData();
            for (SeaTunnelTransform<T> t : transform) {
                event = t.mapSchemaChangeEvent(event);
                if (event == null) {
                    break;
                }
            }
            if (event != null) {
                collector.collect(new Record<>(event));
            }
        } else {
            if (prepareClose) {
                return;
            }
            T inputData = (T) record.getData();
            T outputData = inputData;
            for (SeaTunnelTransform<T> t : transform) {
                outputData = t.map(inputData);
                log.debug("Transform[{}] input row {} and output row {}", t, inputData, outputData);
                if (outputData == null) {
                    log.trace("Transform[{}] filtered data row {}", t, inputData);
                    break;
                }

                inputData = outputData;
            }
            if (outputData != null) {
                // todo log metrics
                collector.collect(new Record<>(outputData));
            }
        }
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        // nothing
    }

    @Override
    public void close() throws IOException {
        for (SeaTunnelTransform<T> t : transform) {
            try {
                t.close();
            } catch (Exception e) {
                log.error(
                        "Close transform: {} failed, cause: {}",
                        t.getPluginName(),
                        e.getMessage(),
                        e);
            }
        }
        super.close();
    }
}
