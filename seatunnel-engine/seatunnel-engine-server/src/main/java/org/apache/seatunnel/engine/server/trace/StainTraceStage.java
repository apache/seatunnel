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

package org.apache.seatunnel.engine.server.trace;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Enumerates the stage codes that can be recorded inside a stain trace payload. */
public enum StainTraceStage {
    // Basic 6 pipeline stages
    SOURCE_EMIT((byte) 1),
    QUEUE_IN((byte) 2),
    QUEUE_OUT((byte) 3),
    TRANSFORM_IN((byte) 4),
    TRANSFORM_OUT((byte) 5),
    SINK_WRITE_DONE((byte) 6),
    // Performance tracing stages (100-series)
    SOURCE_READ_END((byte) 101),
    QUEUE_OFFER_START((byte) 102),
    QUEUE_DESERIALIZE_END((byte) 103),
    TRANSFORM_EXECUTE_START((byte) 104),
    TRANSFORM_EXECUTE_END((byte) 105),
    SINK_BATCH_AGGREGATE_END((byte) 106),
    SINK_FORMAT_END((byte) 107),
    SINK_WRITE_START((byte) 108),
    SINK_WRITE_END((byte) 109),
    SINK_COMMIT_END((byte) 110),
    // Fine-grained stages (200-series)
    SOURCE_READ_START((byte) 201),
    SOURCE_SERIALIZE_START((byte) 202),
    SOURCE_SERIALIZE_END((byte) 203),
    QUEUE_DESERIALIZE_START((byte) 204),
    TRANSFORM_PARSE_START((byte) 205),
    TRANSFORM_PARSE_END((byte) 206),
    TRANSFORM_BUILD_START((byte) 207),
    TRANSFORM_BUILD_END((byte) 208),
    SINK_RECEIVE((byte) 209),
    SINK_BATCH_AGGREGATE_START((byte) 210),
    SINK_FORMAT_START((byte) 211),
    SINK_COMMIT_START((byte) 212),
    CHECKPOINT_SNAPSHOT_START((byte) 213),
    CHECKPOINT_SNAPSHOT_END((byte) 214),
    CHECKPOINT_BARRIER_EMIT((byte) 215),
    CHECKPOINT_BARRIER_RECEIVE((byte) 216),
    // Network transmission stages
    RECORD_SERIALIZE_START((byte) 217),
    RECORD_SERIALIZE_END((byte) 218),
    RECORD_DESERIALIZE_START((byte) 219),
    RECORD_DESERIALIZE_END((byte) 220),
    // Flow control stages
    FLOW_CONTROL_AUDIT_START((byte) 226),
    FLOW_CONTROL_AUDIT_END((byte) 227);

    private static final Map<Integer, StainTraceStage> CODE_MAP = new HashMap<>();

    static {
        for (StainTraceStage s : values()) {
            CODE_MAP.put(s.code & 0xFF, s);
        }
    }

    private final byte code;

    StainTraceStage(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    /** Resolves the numeric stage code stored in the payload back to the matching enum value. */
    public static Optional<StainTraceStage> fromCode(int code) {
        return Optional.ofNullable(CODE_MAP.get(code));
    }
}
