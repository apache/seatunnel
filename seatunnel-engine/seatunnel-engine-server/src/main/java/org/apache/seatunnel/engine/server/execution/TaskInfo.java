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

import static org.apache.seatunnel.engine.server.dag.physical.PhysicalPlanGenerator.getTaskIndex;
import static org.apache.seatunnel.engine.server.dag.physical.PhysicalPlanGenerator.getTaskVertexId;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class TaskInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Long jobId;

    private final Long taskGroupId;

    private final Long subtaskId;

    private final Long jobVertexId;

    private final Integer index;

    public TaskInfo(Long jobId, Long taskGroupId, Long subtaskId) {
        this.jobId = jobId;
        this.taskGroupId = taskGroupId;
        this.subtaskId = subtaskId;
        this.jobVertexId = getTaskVertexId(subtaskId);
        this.index = getTaskIndex(subtaskId);
    }
}
