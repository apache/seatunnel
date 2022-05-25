/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.api.checkpoint;

import org.apache.seatunnel.engine.api.common.JobID;

import java.util.HashMap;

public class MemoryStateBackend implements StateBackend {

    HashMap<JobID, HashMap<Integer, byte[]>> stateMap = new HashMap<>();

    @Override
    public void set(JobID jobId, int taskId, byte[] state) {
        if (stateMap.containsKey(jobId)) {
            stateMap.get(jobId).put(taskId, state);
        } else {
            HashMap<Integer, byte[]> jobState = new HashMap<>();
            jobState.put(taskId, state);
            stateMap.put(jobId, jobState);
        }
    }

    @Override
    public byte[] get(JobID jobId, int taskId) {
        return stateMap.containsKey(jobId) ? stateMap.get(jobId).get(taskId) : null;
    }
}
