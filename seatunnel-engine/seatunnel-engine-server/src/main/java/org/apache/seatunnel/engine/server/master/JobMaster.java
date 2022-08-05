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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlanUtils;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.Task;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;
import lombok.NonNull;

import java.io.IOException;

public class JobMaster implements Task {

    private final LogicalDag logicalDag;
    private PhysicalPlan physicalPlan;

    private NodeEngine nodeEngine;

    public JobMaster() {
        this.logicalDag = new LogicalDag();
    }

    @Override
    public void init() throws Exception {
        physicalPlan = PhysicalPlanUtils.fromLogicalDAG(logicalDag, nodeEngine, System.currentTimeMillis());
    }

    @NonNull
    @Override
    public ProgressState call() {
        return ProgressState.DONE;
    }

    @NonNull
    @Override
    public Long getTaskID() {
        return null;
    }

    @Override
    public void close() throws IOException {
        Task.super.close();
    }

    @Override
    public void setOperationService(OperationService operationService) {
        Task.super.setOperationService(operationService);
    }
}
