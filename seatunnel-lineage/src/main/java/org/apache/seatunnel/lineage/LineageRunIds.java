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

package org.apache.seatunnel.lineage;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Deterministic run identity helpers. */
public final class LineageRunIds {
    private static final String PREFIX = "seatunnel-job:";

    private LineageRunIds() {}

    /** Returns a deterministic run ID for a numeric SeaTunnel job ID. */
    public static UUID forJob(long jobId, String namespace, String name, String attempt) {
        return forJob(String.valueOf(jobId), namespace, name, attempt);
    }

    /** Returns a deterministic run ID for a job and dataset identity. */
    public static UUID forJob(String jobId, String namespace, String name, String attempt) {
        StringBuilder value =
                new StringBuilder(PREFIX)
                        .append(jobId)
                        .append(':')
                        .append(namespace)
                        .append('/')
                        .append(name);
        if (attempt != null && !attempt.trim().isEmpty()) {
            value.append(':').append(attempt);
        }
        // The prefix isolates this type-3 UUID namespace from other emitters that use the same
        // nameUUIDFromBytes algorithm. It must not be removed as redundant.
        return UUID.nameUUIDFromBytes(value.toString().getBytes(StandardCharsets.UTF_8));
    }
}
