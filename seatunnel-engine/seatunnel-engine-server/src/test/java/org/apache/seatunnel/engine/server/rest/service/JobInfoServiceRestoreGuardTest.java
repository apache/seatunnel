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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.RestoreMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Verifies the restore-source guard used by the REST submit path: a restore is refused while the
 * source job is still active, and allowed once the source job has ended or is unknown, so the
 * checkpoint lookup can report missing state with its own error.
 */
public class JobInfoServiceRestoreGuardTest {

    private static final long SOURCE_JOB_ID = 42L;

    @ParameterizedTest
    @EnumSource(
            value = JobStatus.class,
            names = {
                "INITIALIZING",
                "CREATED",
                "PENDING",
                "SCHEDULED",
                "RUNNING",
                "FAILING",
                "DOING_SAVEPOINT",
                "CANCELING"
            })
    public void testActiveSourceJobIsRejected(JobStatus status) {
        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                JobInfoService.rejectRestoreFromActiveSourceJob(
                                        RestoreMode.CHECKPOINT, SOURCE_JOB_ID, status));
        Assertions.assertTrue(
                error.getMessage().contains("restoreSourceJobId=42"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains(status.name()), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("checkpoint state"), error.getMessage());
    }

    @ParameterizedTest
    @EnumSource(
            value = JobStatus.class,
            names = {"FAILED", "SAVEPOINT_DONE", "CANCELED", "FINISHED", "UNKNOWABLE"})
    public void testEndedOrUnknownSourceJobIsAccepted(JobStatus status) {
        Assertions.assertDoesNotThrow(
                () ->
                        JobInfoService.rejectRestoreFromActiveSourceJob(
                                RestoreMode.SAVEPOINT, SOURCE_JOB_ID, status));
    }

    @Test
    public void testNullStatusIsAccepted() {
        Assertions.assertDoesNotThrow(
                () ->
                        JobInfoService.rejectRestoreFromActiveSourceJob(
                                RestoreMode.SAVEPOINT, SOURCE_JOB_ID, null));
    }

    @Test
    public void testSavepointModeIsNamedInTheRejection() {
        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                JobInfoService.rejectRestoreFromActiveSourceJob(
                                        RestoreMode.SAVEPOINT, SOURCE_JOB_ID, JobStatus.RUNNING));
        Assertions.assertTrue(error.getMessage().contains("savepoint state"), error.getMessage());
    }
}
