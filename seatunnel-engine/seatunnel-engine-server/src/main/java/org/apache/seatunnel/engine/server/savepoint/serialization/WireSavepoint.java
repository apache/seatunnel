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

import java.util.Map;

/**
 * Wire-format payload of a completed checkpoint/savepoint (format {@code engine-wire-v1}).
 *
 * <p>This DTO is the long-term storage contract. It intentionally does <b>not</b> mirror the
 * runtime {@link org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint} class -
 * runtime-only fields (e.g. {@code isRestored}) are excluded, and enums are encoded by stable name
 * string instead of ordinal/reflection order.
 *
 * <p>Field numbers are frozen by {@link Tag}: append new fields with new tag numbers only;
 * structural changes require a format version bump plus a versioned reader.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WireSavepoint {

    @Tag(1)
    private long checkpointId;

    @Tag(2)
    private int pipelineId;

    @Tag(3)
    private long jobId;

    @Tag(4)
    private long triggerTimestamp;

    /**
     * Stable {@link org.apache.seatunnel.engine.core.checkpoint.CheckpointType#getName()} value.
     */
    @Tag(5)
    private String checkpointTypeName;

    @Tag(6)
    private long completedTimestamp;

    /** Keyed by {@link org.apache.seatunnel.engine.server.checkpoint.ActionStateKey#getName()}. */
    @Tag(7)
    private Map<String, WireActionState> taskStates;

    @Tag(8)
    private Map<Long, WireTaskStatistics> taskStatistics;
}
