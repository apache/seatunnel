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

package org.apache.seatunnel.engine.server.savepoint.serialization;

import io.protostuff.Tag;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/** Wire-format DTO of task statistics ({@code engine-wire-v1}). */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WireTaskStatistics {

    @Tag(1)
    private Long jobVertexId;

    /** Null elements are preserved (parallelism slots without stats). */
    @Tag(2)
    private List<WireSubtaskStatistics> subtaskStats;

    @Tag(3)
    private boolean[] subtaskCompleted;

    /** Derived: equals the number of non-null entries in {@link #subtaskStats}. */
    @Tag(4)
    private int numAcknowledgedSubtasks;

    @Tag(5)
    private WireSubtaskStatistics latestAckedSubtaskStatistics;
}
