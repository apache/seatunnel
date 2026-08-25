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

package org.apache.seatunnel.engine.checkpoint.storage.savepoint;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

/** Request for starting a savepoint write attempt. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SavepointRequest {

    /** Logical job id. */
    private String jobId;

    /** Globally unique savepoint id, shared by all pipelines of the bundle. */
    private String savepointId;

    /** Per-attempt id; staging is isolated per attempt to make retries safe. */
    private String attemptId;

    /**
     * Expected pipeline ids of the bundle. When present, commit requires exactly this set to have
     * been written - a bundle that is missing any pipeline (or contains an unknown one) must not be
     * published.
     */
    private Set<Integer> expectedPipelineIds;
}
