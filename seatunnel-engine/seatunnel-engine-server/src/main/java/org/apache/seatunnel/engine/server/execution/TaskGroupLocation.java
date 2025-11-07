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

import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class TaskGroupLocation implements Serializable {
    private static final long serialVersionUID = -8321526709920799751L;
    private final long jobId;

    private final int pipelineId;

    private final long taskGroupId;

    public PipelineLocation getPipelineLocation() {
        return new PipelineLocation(this.jobId, this.pipelineId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TaskGroupLocation that = (TaskGroupLocation) o;

        return new EqualsBuilder()
                .append(jobId, that.jobId)
                .append(pipelineId, that.pipelineId)
                .append(taskGroupId, that.taskGroupId)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(jobId)
                .append(pipelineId)
                .append(taskGroupId)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "TaskGroupLocation{"
                + "jobId="
                + jobId
                + ", pipelineId="
                + pipelineId
                + ", taskGroupId="
                + taskGroupId
                + '}';
    }
}
