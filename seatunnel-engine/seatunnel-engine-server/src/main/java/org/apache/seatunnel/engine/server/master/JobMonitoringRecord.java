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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.engine.common.job.JobStatus;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/** Lightweight terminal-job record ordered by its actual insertion sequence. */
@AllArgsConstructor
@Data
public final class JobMonitoringRecord implements Serializable {

    private static final long serialVersionUID = -6843786249916106271L;

    // Monotonic ledger position; zero is reserved for a pending outbox record.
    private long sequence;

    // Stable SeaTunnel job identifier.
    private Long jobId;

    // Bounded display name used by monitoring responses.
    private String jobName;

    // Terminal job status captured by the authoritative history write.
    private JobStatus jobStatus;

    // Original job submission timestamp.
    private long submitTime;

    // Optional job start timestamp.
    private Long startTime;

    // Optional terminal timestamp, which can be absent during abnormal recovery.
    private Long finishTime;

    // Non-null timestamp at which the sidecar observed the terminal state.
    private long observedTime;

    // Bounded error detail suitable for alert payloads.
    private String errorSummary;
}
