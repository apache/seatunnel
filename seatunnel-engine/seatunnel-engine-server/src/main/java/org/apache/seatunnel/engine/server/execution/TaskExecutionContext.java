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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.util.HashMap;

public class TaskExecutionContext {

    private final Task task;
    private final NodeEngineImpl nodeEngine;
    private final TaskExecutionService taskExecutionService;

    public TaskExecutionContext(
            Task task, NodeEngineImpl nodeEngine, TaskExecutionService taskExecutionService) {
        this.task = task;
        this.nodeEngine = nodeEngine;
        this.taskExecutionService = taskExecutionService;
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

    public SeaTunnelMetricsContext getOrCreateMetricsContext(TaskLocation taskLocation) {
        IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> map =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_METRICS);
        HashMap<TaskLocation, SeaTunnelMetricsContext> centralMap =
                map.get(Constant.IMAP_RUNNING_JOB_METRICS_KEY);
        return centralMap == null || centralMap.get(taskLocation) == null
                ? new SeaTunnelMetricsContext()
                : centralMap.get(taskLocation);
    }

    public <T> T getTask() {
        return (T) task;
    }

    public TaskExecutionService getTaskExecutionService() {
        return taskExecutionService;
    }

    public HazelcastInstance getInstance() {
        return nodeEngine.getHazelcastInstance();
    }
}
