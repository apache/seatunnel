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
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.engine.core.dag.actions.PartitionTransformAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.core.dag.actions.UnknownActionException;
import org.apache.seatunnel.engine.server.dag.physical.flow.Flow;
import org.apache.seatunnel.engine.server.dag.physical.flow.IntermediateExecutionFlow;
import org.apache.seatunnel.engine.server.dag.physical.flow.PhysicalExecutionFlow;
import org.apache.seatunnel.engine.server.dag.physical.flow.UnknownFlowException;
import org.apache.seatunnel.engine.server.task.flow.FlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.IntermediateQueueFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.OneInputFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.PartitionTransformSinkFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.PartitionTransformSourceFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.SinkFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.SourceFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.TransformFlowLifeCycle;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

public abstract class SeaTunnelTask<R> extends AbstractTask {

    private static final long serialVersionUID = 2604309561613784425L;
    private final Flow executionFlow;

    private int enumeratorTaskID = -1;

    protected FlowLifeCycle startFlowLifeCycle;

    protected List<OneInputFlowLifeCycle<R>> outputs;

    // TODO add index ID
    private int indexID;

    public SeaTunnelTask(int taskID, Flow executionFlow) {
        super(taskID);
        // TODO add enumerator task ID
        enumeratorTaskID = 1;
        this.executionFlow = executionFlow;
    }

    @Override
    public void init() throws Exception {
        startFlowLifeCycle = convertFlowToActionLifeCycle(executionFlow);
        // TODO init outputs
        progress.makeProgress();
    }

    @SuppressWarnings({"unchecked", "rawtypes", "checkstyle:MagicNumber"})
    private FlowLifeCycle convertFlowToActionLifeCycle(Flow flow) throws Exception {

        FlowLifeCycle lifeCycle;

        Collector<Record> collector = null;
        if (!executionFlow.getNext().isEmpty()) {
            List<OneInputFlowLifeCycle<Record>> flowLifeCycles = new ArrayList<>();

            for (Flow f : executionFlow.getNext()) {
                flowLifeCycles.add((OneInputFlowLifeCycle<Record>) convertFlowToActionLifeCycle(f));
            }
            collector = new SeaTunnelTransformCollector<>(flowLifeCycles);
        }
        if (flow instanceof PhysicalExecutionFlow) {
            PhysicalExecutionFlow f = (PhysicalExecutionFlow) flow;
            if (f.getAction() instanceof SourceAction) {
                lifeCycle = new SourceFlowLifeCycle<>((SourceAction) f.getAction(), indexID,
                        enumeratorTaskID, this, taskID);
            } else if (f.getAction() instanceof SinkAction) {
                lifeCycle = new SinkFlowLifeCycle<>((SinkAction) f.getAction(), indexID);
            } else if (f.getAction() instanceof TransformChainAction) {
                lifeCycle =
                        new TransformFlowLifeCycle<SeaTunnelRow, Record>(((TransformChainAction) f.getAction()).getTransforms(),
                                collector);
            } else if (f.getAction() instanceof PartitionTransformAction) {
                // TODO use index and taskID to create ringbuffer list
                if (executionFlow.getNext().isEmpty()) {
                    lifeCycle = new PartitionTransformSinkFlowLifeCycle<>();
                } else {
                    lifeCycle = new PartitionTransformSourceFlowLifeCycle<>();
                }
            } else {
                throw new UnknownActionException(f.getAction());
            }
        } else if (flow instanceof IntermediateExecutionFlow) {
            // TODO The logic here needs to be adjusted to satisfy the input and output using the same queue
            lifeCycle = new IntermediateQueueFlowLifeCycle<>(new ArrayBlockingQueue<>(100));
        } else {
            throw new UnknownFlowException(flow);
        }
        lifeCycle.init();
        return lifeCycle;
    }

    @Override
    public void close() throws IOException {
        // TODO regress close
        startFlowLifeCycle.close();
        progress.done();
    }

    @Override
    public void receivedMessage(Object message) {
        // TODO send to which flow?
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
