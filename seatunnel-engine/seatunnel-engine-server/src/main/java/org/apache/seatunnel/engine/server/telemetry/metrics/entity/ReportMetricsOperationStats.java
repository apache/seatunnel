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

package org.apache.seatunnel.engine.server.telemetry.metrics.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/** Snapshot of worker-side ReportMetricsOperation observability state. */
@Data
@AllArgsConstructor
public class ReportMetricsOperationStats {
    /** Total successful report invocations sent by this worker. */
    private long successCount;
    /** Total failed report invocations sent by this worker. */
    private long failureCount;
    /** Total interrupted report invocations sent by this worker. */
    private long interruptedCount;
    /** Task metric count in the most recent report payload. */
    private long lastPayloadTaskCount;
    /** Most recent worker-side reporting latency in milliseconds. */
    private long lastInvocationLatencyMs;
    /** Maximum observed worker-side reporting latency in milliseconds. */
    private long maxInvocationLatencyMs;
}
