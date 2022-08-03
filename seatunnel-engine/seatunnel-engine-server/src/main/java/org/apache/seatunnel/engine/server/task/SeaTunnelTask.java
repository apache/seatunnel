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

import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalExecutionFlow;
import org.apache.seatunnel.engine.server.task.operation.RegisterOperation;
import org.apache.seatunnel.engine.server.task.operation.RequestSplitOperation;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class SeaTunnelTask extends AbstractTask {

    private static final long serialVersionUID = 2604309561613784425L;
    private final PhysicalExecutionFlow executionFlow;

    // TODO init memberID in task execution service
    private UUID memberID = UUID.randomUUID();
    // TODO add master address
    private Address master = new Address();

    public SeaTunnelTask(long taskID, PhysicalExecutionFlow executionFlow) {
        super(taskID);
        this.executionFlow = executionFlow;
    }

    @Override
    public void init() {
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    private void register() {
        if (startFromSource()) {
            operationService.invokeOnTarget(SeaTunnelServer.SERVICE_NAME, new RegisterOperation(taskID, memberID),
                    master);
        }
    }

    private void requestSplit() {
        operationService.invokeOnTarget(SeaTunnelServer.SERVICE_NAME, new RequestSplitOperation<>(taskID,
                memberID), master);
    }

    private boolean startFromSource() {
        return executionFlow.getAction() instanceof SourceAction;
    }

    @Override
    public Set<URL> getJarsUrl() {
        List<PhysicalExecutionFlow> now = Collections.singletonList(executionFlow);
        Set<URL> urls = new HashSet<>();
        List<PhysicalExecutionFlow> next = new ArrayList<>();
        while (!now.isEmpty()) {
            next.clear();
            now.forEach(n -> {
                urls.addAll(n.getAction().getJarUrls());
                next.addAll(n.getNext());
            });
            now = next;
        }
        return urls;
    }

    @Override
    public void setOperationService(OperationService operationService) {
        this.operationService = operationService;
    }
}
