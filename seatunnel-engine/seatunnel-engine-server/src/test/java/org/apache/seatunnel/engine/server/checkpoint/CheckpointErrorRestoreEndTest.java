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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@DisabledOnOs(OS.WINDOWS)
public class CheckpointErrorRestoreEndTest
        extends AbstractSeaTunnelServerTest<CheckpointErrorRestoreEndTest> {
    public static String STREAM_CONF_WITH_ERROR_PATH =
            "batch_fakesource_to_inmemory_with_commit_error.conf";
    /**
     * The commit-error batch job restores three times before reaching the final FAILED state, so
     * the related regression tests share the same 240-second upper bound.
     */
    public static final long RESTORE_TO_FAILED_TIMEOUT_SECONDS = 240L;

    public static String STREAM_CONF_WITH_ERROR_AND_LONG_RESTORE_WAIT_PATH =
            "stream_fake_to_inmemory_with_error_long_restore_wait.conf";

    @Test
    public void testCheckpointRestoreToFailEnd() {
        long jobId = System.currentTimeMillis();
        startJob(jobId, STREAM_CONF_WITH_ERROR_PATH, false);

        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);
        Assertions.assertEquals(1, jobMaster.getPhysicalPlan().getPipelineList().size());
        await().atMost(RESTORE_TO_FAILED_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        3,
                                        jobMaster
                                                .getPhysicalPlan()
                                                .getPipelineList()
                                                .get(0)
                                                .getPipelineRestoreNum()));
        await().atMost(RESTORE_TO_FAILED_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FAILED,
                                        server.getCoordinatorService().getJobStatus(jobId)));
    }

    /**
     * A cancel that arrives while a failed pipeline waits for its restore ends the job {@code
     * CANCELED}, as it did when the pipeline restarted first and was then cancelled. The pipeline
     * fails its first checkpoint and then waits 30 s, so the cancel lands inside that wait.
     */
    @Test
    public void testCancelDuringPipelineRestoreWaitEndsTheJobCanceled() {
        long jobId = System.currentTimeMillis();
        startJob(jobId, STREAM_CONF_WITH_ERROR_AND_LONG_RESTORE_WAIT_PATH, false);

        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);
        SubPlan pipeline = jobMaster.getPhysicalPlan().getPipelineList().get(0);
        // reset for its first restore and not restarted yet
        await().atMost(60, TimeUnit.SECONDS)
                .until(
                        () ->
                                pipeline.getPipelineRestoreNum() == 1
                                        && PipelineStatus.CREATED.equals(
                                                pipeline.getPipelineState()));
        server.getCoordinatorService().cancelJob(jobId);

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.CANCELED,
                                        server.getCoordinatorService().getJobStatus(jobId)));
    }
}
