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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.RestoreMode;

import java.util.function.LongFunction;

final class CheckpointRestoreValidator {

    private CheckpointRestoreValidator() {}

    static void validate(
            JobImmutableInformation jobImmutableInformation,
            long destinationJobId,
            LongFunction<JobStatus> activeSourceJobStatusResolver) {
        if (jobImmutableInformation == null
                || jobImmutableInformation.getRestoreMode() != RestoreMode.CHECKPOINT) {
            return;
        }

        Long restoreSourceJobId = jobImmutableInformation.getRestoreSourceJobId();
        if (restoreSourceJobId == null) {
            throw new JobException(
                    "restoreSourceJobId is required when restoreMode="
                            + jobImmutableInformation.getRestoreMode());
        }
        if (restoreSourceJobId == destinationJobId) {
            throw new JobException(
                    "restoreSourceJobId must reference a historical terminal source job when restoreMode=CHECKPOINT");
        }

        JobStatus sourceJobStatus =
                activeSourceJobStatusResolver == null
                        ? null
                        : activeSourceJobStatusResolver.apply(restoreSourceJobId);
        if (sourceJobStatus != null && !sourceJobStatus.isEndState()) {
            throw new JobException(
                    String.format(
                            "checkpoint restore requires a terminal source job, restoreSourceJobId=%s, current source job status=%s",
                            restoreSourceJobId, sourceJobStatus));
        }
    }
}
