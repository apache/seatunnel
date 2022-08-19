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

public class TaskInfo {

    private final Long jobId;

    private final Long pipelineId;

    private final Long jobVertexId;

    private final Integer index;

    public TaskInfo(Long jobId, Long pipelineId, Long jobVertexId, Integer index) {
        this.jobId = jobId;
        this.pipelineId = pipelineId;
        this.jobVertexId = jobVertexId;
        this.index = index;
    }

    public Long getJobId() {
        return jobId;
    }

    public Long getPipelineId() {
        return pipelineId;
    }

    public Long getJobVertexId() {
        return jobVertexId;
    }

    public Integer getIndex() {
        return index;
    }
}
