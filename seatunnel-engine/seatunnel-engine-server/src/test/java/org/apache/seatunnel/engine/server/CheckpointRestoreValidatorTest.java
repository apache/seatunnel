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

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.RestoreMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CheckpointRestoreValidatorTest {

    @Test
    void shouldRejectCheckpointRestoreWhenSourceJobIsStillRunning() {
        long sourceJobId = 1001L;

        JobException exception =
                Assertions.assertThrows(
                        JobException.class,
                        () ->
                                CheckpointRestoreValidator.validate(
                                        newRestoreJob(sourceJobId),
                                        1002L,
                                        ignored -> JobStatus.RUNNING));

        Assertions.assertTrue(
                exception.getMessage().contains("terminal source job"),
                () -> "message=" + exception.getMessage());
        Assertions.assertTrue(
                exception.getMessage().contains(JobStatus.RUNNING.name()),
                () -> "message=" + exception.getMessage());
    }

    @Test
    void shouldAllowCheckpointRestoreWhenSourceJobIsTerminal() {
        long sourceJobId = 1001L;

        Assertions.assertDoesNotThrow(
                () ->
                        CheckpointRestoreValidator.validate(
                                newRestoreJob(sourceJobId), 1002L, ignored -> JobStatus.CANCELED));
    }

    private JobImmutableInformation newRestoreJob(long sourceJobId) {
        JobImmutableInformation jobImmutableInformation = new JobImmutableInformation();
        ReflectionUtils.setField(
                jobImmutableInformation,
                JobImmutableInformation.class,
                "restoreMode",
                RestoreMode.CHECKPOINT);
        ReflectionUtils.setField(
                jobImmutableInformation,
                JobImmutableInformation.class,
                "restoreSourceJobId",
                sourceJobId);
        return jobImmutableInformation;
    }
}
