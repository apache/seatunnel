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

package org.apache.seatunnel.engine.common.config.spec;

import lombok.Builder;
import lombok.Getter;

import java.util.Objects;

/**
 * Immutable, platform-independent settings for one native application job.
 *
 * <p>The submitter resolves these fields once. The platform carries them to the master, which
 * injects the specification into the job runner and resource manager. This object contains no
 * original file paths, platform options, configuration lookup or process lifecycle logic.
 */
@Getter
public final class ApplicationSpecification {
    private final String name;
    /** Resolved job content; may contain credentials and must not be logged. */
    private final String jobConfig;

    /** Native Zeta execution identity; distinct from the platform's application/container ID. */
    private final long jobId;
    /** Historical job whose checkpoint should be restored, or null for a fresh execution. */
    private final Long restoreJobId;
    /** Fixed application capacity; this is not an autoscaling policy. */
    private final int workerCount;

    private final WorkerSpecification workerSpecification;
    private final int masterMemoryMb;
    private final int masterCpuCores;
    private final int masterPort;
    private final long startupTimeoutMillis;

    /**
     * Validates resolved application settings without reading configuration or allocating
     * resources.
     */
    @Builder(toBuilder = true)
    public ApplicationSpecification(
            String name,
            String jobConfig,
            long jobId,
            Long restoreJobId,
            int workerCount,
            WorkerSpecification workerSpecification,
            int masterMemoryMb,
            int masterCpuCores,
            int masterPort,
            long startupTimeoutMillis) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Application name must not be empty");
        }
        if (jobConfig == null || jobConfig.trim().isEmpty()) {
            throw new IllegalArgumentException("Application job configuration must not be empty");
        }
        if (jobId <= 0 || (restoreJobId != null && restoreJobId <= 0)) {
            throw new IllegalArgumentException("Application job IDs must be positive");
        }
        if (restoreJobId != null && restoreJobId == jobId) {
            throw new IllegalArgumentException(
                    "application.job-id must differ from application.restore-job-id to preserve the source checkpoint");
        }
        if (workerCount <= 0) {
            throw new IllegalArgumentException("application.worker-count must be positive");
        }
        if (startupTimeoutMillis <= 0 || masterMemoryMb <= 0 || masterCpuCores <= 0) {
            throw new IllegalArgumentException(
                    "Application startup timeout and master resources must be positive");
        }
        if (masterPort < 1 || masterPort > 65535) {
            throw new IllegalArgumentException(
                    "application.master.port must be between 1 and 65535");
        }
        this.name = name;
        this.jobConfig = jobConfig;
        this.jobId = jobId;
        this.restoreJobId = restoreJobId;
        this.workerCount = workerCount;
        this.workerSpecification =
                Objects.requireNonNull(workerSpecification, "workerSpecification");
        this.masterMemoryMb = masterMemoryMb;
        this.masterCpuCores = masterCpuCores;
        this.masterPort = masterPort;
        this.startupTimeoutMillis = startupTimeoutMillis;
    }
}
