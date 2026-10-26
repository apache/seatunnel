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

package org.apache.seatunnel.engine.common.job;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
@Data
@NoArgsConstructor
public final class JobStatusData implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long jobId;
    private String jobName;
    private JobStatus jobStatus;
    private long submitTime;
    private Long startTime;
    private Long finishTime;
    // Legacy CLI readers reject unknown JSON fields; REST projects this state explicitly.
    @JsonIgnore private DirtyJobSummary dirtyTask;

    public JobStatusData(
            Long jobId,
            String jobName,
            JobStatus jobStatus,
            long submitTime,
            Long startTime,
            Long finishTime) {
        this(jobId, jobName, jobStatus, submitTime, startTime, finishTime, null);
    }
}
