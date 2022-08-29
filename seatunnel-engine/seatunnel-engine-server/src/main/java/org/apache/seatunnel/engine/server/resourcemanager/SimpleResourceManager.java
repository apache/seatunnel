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

package org.apache.seatunnel.engine.server.resourcemanager;

import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.Data;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;

@Data
public class SimpleResourceManager implements ResourceManager {

    // TODO We may need more detailed resource define, instead of the resource definition method of only Address.
    private Map<Long, Map<TaskGroupLocation, Address>> physicalVertexIdAndResourceMap = new HashMap<>();

    private final NodeEngine nodeEngine;

    public SimpleResourceManager(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public Address applyForResource(long jobId, TaskGroupLocation taskGroupLocation) {
        Map<TaskGroupLocation, Address> jobAddressMap =
            physicalVertexIdAndResourceMap.computeIfAbsent(jobId, k -> new HashMap<>());

        Address localhost =
            jobAddressMap.putIfAbsent(taskGroupLocation, nodeEngine.getThisAddress());
        if (null == localhost) {
            localhost = jobAddressMap.get(taskGroupLocation);
        }

        return localhost;

    }

    @Override
    @NonNull
    public Address getAppliedResource(long jobId, TaskGroupLocation taskGroupLocation) {
        Map<TaskGroupLocation, Address> longAddressMap = physicalVertexIdAndResourceMap.get(jobId);
        if (null == longAddressMap || longAddressMap.isEmpty()) {
            throw new JobException(
                String.format("Job %s, Task %s can not found applied resource.", jobId, taskGroupLocation));
        }

        return longAddressMap.get(taskGroupLocation);
    }
}
