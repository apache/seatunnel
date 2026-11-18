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

package org.apache.seatunnel.resource.yarn.launch.resources;

import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;

import org.apache.hadoop.yarn.api.records.Resource;

import lombok.Value;

/** Memory in MiB and virtual cores requested from YARN for one container. */
@Value
public class YarnResourceRequirements {
    int memoryMb;
    int virtualCores;

    public static YarnResourceRequirements forMaster(ApplicationSpecification application) {
        return new YarnResourceRequirements(
                application.getMasterMemoryMb(), application.getMasterCpuCores());
    }

    public static YarnResourceRequirements forWorker(WorkerSpecification worker) {
        return new YarnResourceRequirements(worker.getMemoryMb(), worker.getCpuCores());
    }

    public boolean exceeds(Resource maximum) {
        return memoryMb > maximum.getMemorySize() || virtualCores > maximum.getVirtualCores();
    }

    public Resource toResource() {
        return Resource.newInstance(memoryMb, virtualCores);
    }
}
