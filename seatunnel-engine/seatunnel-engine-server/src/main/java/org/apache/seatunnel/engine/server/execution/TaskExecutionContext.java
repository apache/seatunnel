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

package org.apache.seatunnel.engine.server.execution;

import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

public class TaskExecutionContext {

    private final Task task;
    private final NodeEngineImpl nodeEngine;

    public TaskExecutionContext(Task task, NodeEngineImpl nodeEngine) {
        this.task = task;
        this.nodeEngine = nodeEngine;
    }

    public <E> InvocationFuture<E> sendToMaster(Operation operation) {
        return NodeEngineUtil.sendOperationToMasterNode(nodeEngine, operation);
    }

    public <E> InvocationFuture<E> sendToMember(Operation operation, Address memberID) {
        return NodeEngineUtil.sendOperationToMemberNode(nodeEngine, operation, memberID);
    }

    public ILogger getLogger() {
        return nodeEngine.getLogger(task.getClass());
    }

    public <T> T getTask() {
        return (T) task;
    }

}
