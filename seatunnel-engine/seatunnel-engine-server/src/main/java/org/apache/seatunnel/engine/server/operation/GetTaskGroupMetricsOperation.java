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

package org.apache.seatunnel.engine.server.operation;

import org.apache.seatunnel.api.common.metrics.RawJobMetrics;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.metrics.JobMetricsCollector;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;

public class GetTaskGroupMetricsOperation extends Operation {

    private TaskGroupLocation taskGroupLocation;
    private RawJobMetrics response;

    public GetTaskGroupMetricsOperation(TaskGroupLocation taskGroupLocation) {
        this.taskGroupLocation = taskGroupLocation;
    }

    @Override
    public void run() {
        ILogger logger = getLogger();

        Address callerAddress = getCallerAddress();

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Address masterAddress = getNodeEngine().getMasterAddress();
        if (!callerAddress.equals(masterAddress)) {
            throw new IllegalStateException("Caller " + callerAddress + " cannot get taskGroupLocation metrics"
                    + taskGroupLocation.toString() + " because it is not master. Master is: " + masterAddress);
        }

        JobMetricsCollector
            metricsRenderer = new JobMetricsCollector(taskGroupLocation, nodeEngine.getLocalMember(), logger);
        nodeEngine.getMetricsRegistry().collect(metricsRenderer);
        response = metricsRenderer.getMetrics();
    }

    @Override
    public Object getResponse() {
        return response;
    }
}
