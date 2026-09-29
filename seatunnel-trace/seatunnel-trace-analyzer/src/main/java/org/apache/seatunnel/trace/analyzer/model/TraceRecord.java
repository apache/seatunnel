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

package org.apache.seatunnel.trace.analyzer.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/** Represents one analyzed trace span enriched with task and table metadata. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TraceRecord {
    private long traceId;
    private long sinkTaskId;
    private String jobId;
    private String tableId;
    private long createdTime;
    private List<TraceEntry> entries;

    /**
     * Computes the end-to-end latency by subtracting the first stage timestamp from the last one.
     */
    public long getE2ELatencyMs() {
        if (entries == null || entries.size() < 2) {
            return 0;
        }
        long firstTs = entries.get(0).getTimestampMs();
        long lastTs = entries.get(entries.size() - 1).getTimestampMs();
        return lastTs - firstTs;
    }
}
