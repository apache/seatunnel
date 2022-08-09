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

import com.hazelcast.cluster.Address;
import lombok.Data;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

@Data
public class SimpleResourceManager implements ResourceManager {

    // TODO We may need more detailed resource define, instead of the resource definition method of only Address.
    private Map<Long, Map<Long, Address>> physicalVertexIdAndResourceMap = new HashMap<>();

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public Address applyForResource(Long jobId, Long physicalVertexId) {
        try {
            Map<Long, Address> jobAddressMap = physicalVertexIdAndResourceMap.get(jobId);
            if (jobAddressMap == null) {
                jobAddressMap = new HashMap<>();
                physicalVertexIdAndResourceMap.put(jobId, jobAddressMap);
            }

            Address localhost =
                jobAddressMap.putIfAbsent(physicalVertexId, new Address("localhost", 5801));

            if (null == localhost) {
                localhost = jobAddressMap.get(physicalVertexId);
            }

            return localhost;

        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
