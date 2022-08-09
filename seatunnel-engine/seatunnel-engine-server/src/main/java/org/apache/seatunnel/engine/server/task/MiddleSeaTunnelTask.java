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
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.engine.server.dag.physical.flow.Flow;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.task.flow.OneOutputFlowLifeCycle;

import lombok.NonNull;

public class MiddleSeaTunnelTask extends SeaTunnelTask<Record> {

    public MiddleSeaTunnelTask(int taskID, Flow executionFlow) {
        super(taskID, executionFlow);
    }

    private Collector<Record> collector;

    @Override
    public void init() throws Exception {
        super.init();
        collector = new SeaTunnelTransformCollector<>(outputs);
    }

    @NonNull
    @Override
    @SuppressWarnings("unchecked")
    public ProgressState call() throws Exception {
        if (startFlowLifeCycle instanceof OneOutputFlowLifeCycle) {
            ((OneOutputFlowLifeCycle<Record>) startFlowLifeCycle).collect(collector);
        } else {
            throw new TaskRuntimeException("SourceSeaTunnelTask only support OneOutputFlowLifeCycle, but get " + startFlowLifeCycle.getClass().getName());
        }
        return progress.toState();
    }
}
