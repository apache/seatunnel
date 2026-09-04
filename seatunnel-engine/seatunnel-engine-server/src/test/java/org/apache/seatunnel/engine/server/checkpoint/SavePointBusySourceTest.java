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

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;

/**
 * Regression tests for <a href="https://github.com/apache/seatunnel/issues/11473">#11473</a>:
 * stop-job with savepoint used to hang in {@link JobStatus#DOING_SAVEPOINT} forever when the source
 * was busy emitting a very large split, because the whole split was emitted while holding the
 * checkpoint lock and the savepoint barrier could never be injected.
 */
@DisabledOnOs(OS.WINDOWS)
public class SavePointBusySourceTest extends AbstractSeaTunnelServerTest<SavePointBusySourceTest> {

    /** The reproduction config of issue #11473: one huge FakeSource split into a Console sink. */
    public static final String BUSY_STREAM_CONF_PATH =
            "stream_fakesource_busy_to_console_savepoint.conf";

    /** A large user-configured FakeSource rows list that requires multiple pollNext calls. */
    public static final String BUSY_CUSTOM_ROWS_STREAM_CONF_PATH =
            "stream_fakesource_custom_rows_busy_to_console_savepoint.conf";

    /** A job whose savepoint deterministically times out at the sink. */
    public static final String SAVEPOINT_TIMEOUT_CONF_PATH =
            "stream_fake_to_inmemory_savepoint_timeout.conf";

    /** A lightweight long-running job used to verify savepoint retry after readiness returns. */
    public static final String RETRYABLE_SAVEPOINT_CONF_PATH =
            "stream_fakesource_retryable_to_console_savepoint.conf";

    /** One pipeline completes its savepoint while another pipeline times out. */
    public static final String MULTI_PIPELINE_SAVEPOINT_PARTIAL_FAILURE_CONF_PATH =
            "stream_two_pipelines_savepoint_partial_failure.conf";

    @Test
    public void testStopWithSavepointCompletesWhileSourceEmitsLargeSplit() throws Exception {
        long jobId = System.currentTimeMillis();
        startJob(jobId, BUSY_STREAM_CONF_PATH, false);

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        server.getCoordinatorService().getJobStatus(jobId)));
        awaitCheckpointCoordinatorsReady(server.getCoordinatorService().getJobMaster(jobId));

        // Let the source saturate the pipeline mid-split before requesting the savepoint
        Thread.sleep(5000L);

        // This is the same call the REST stop-job handler makes for isStopWithSavePoint=true
        PassiveCompletableFuture<Void> savepointFuture =
                server.getCoordinatorService().savePoint(jobId);

        // 2. the job passes through DOING_SAVEPOINT and reaches SAVEPOINT_DONE
        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            JobStatus status = server.getCoordinatorService().getJobStatus(jobId);
                            Assertions.assertTrue(
                                    status == JobStatus.DOING_SAVEPOINT
                                            || status == JobStatus.SAVEPOINT_DONE,
                                    "unexpected status " + status);
                        });
        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.SAVEPOINT_DONE,
                                        server.getCoordinatorService().getJobStatus(jobId)));

        // 1./3. the stop-with-savepoint request completes instead of hanging forever
        savepointFuture.get(120, TimeUnit.SECONDS);

        // 4. all slots occupied by the job are released
        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        0,
                                        server.getSlotService()
                                                .getWorkerProfile()
                                                .getAssignedSlots()
                                                .length));
    }

    @Test
    public void testStopWithSavepointCompletesWhileSourceEmitsLargeCustomRows() throws Exception {
        long jobId = System.currentTimeMillis();
        startJob(jobId, BUSY_CUSTOM_ROWS_STREAM_CONF_PATH, false);

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        server.getCoordinatorService().getJobStatus(jobId)));
        awaitCheckpointCoordinatorsReady(server.getCoordinatorService().getJobMaster(jobId));

        // Let the custom rows source enter its paced split loop before requesting the savepoint.
        // The config uses many small splits with a read interval, so this settle window avoids
        // racing the savepoint request against natural job completion.
        Thread.sleep(2000L);

        // This config exercises the user-provided `rows` branch of FakeSourceReader.
        PassiveCompletableFuture<Void> savepointFuture =
                server.getCoordinatorService().savePoint(jobId);

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.SAVEPOINT_DONE,
                                        server.getCoordinatorService().getJobStatus(jobId)));

        savepointFuture.get(120, TimeUnit.SECONDS);

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        0,
                                        server.getSlotService()
                                                .getWorkerProfile()
                                                .getAssignedSlots()
                                                .length));
    }

    @Test
    public void testNormalStopWhileSourceEmitsLargeSplit() {
        long jobId = System.currentTimeMillis();
        startJob(jobId, BUSY_STREAM_CONF_PATH, false);

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        server.getCoordinatorService().getJobStatus(jobId)));

        // 5. a normal stop (no savepoint) still works while the source is busy
        server.getCoordinatorService().cancelJob(jobId);

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.CANCELED,
                                        server.getCoordinatorService().getJobStatus(jobId)));

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        0,
                                        server.getSlotService()
                                                .getWorkerProfile()
                                                .getAssignedSlots()
                                                .length));
    }

    @Test
    public void testSavepointFailureReportsErrorAndStopsJob() throws Exception {
        long jobId = System.currentTimeMillis();
        startJob(jobId, SAVEPOINT_TIMEOUT_CONF_PATH, false);

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        server.getCoordinatorService().getJobStatus(jobId)));
        awaitCheckpointCoordinatorsReady(server.getCoordinatorService().getJobMaster(jobId));

        PassiveCompletableFuture<Void> savepointFuture =
                server.getCoordinatorService().savePoint(jobId);

        // 6. the failed savepoint reports an error to the caller instead of hanging forever
        Assertions.assertThrows(
                ExecutionException.class, () -> savepointFuture.get(120, TimeUnit.SECONDS));

        // and the job does not stay stuck in DOING_SAVEPOINT or report RUNNING after a failed
        // stop-with-savepoint request.
        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(() -> assertFailedSavepointTerminalStatus(jobId));
        Assertions.assertNotEquals(
                JobStatus.RUNNING, server.getCoordinatorService().getJobStatus(jobId));
        Assertions.assertNotEquals(
                JobStatus.DOING_SAVEPOINT, server.getCoordinatorService().getJobStatus(jobId));

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        0,
                                        server.getSlotService()
                                                .getWorkerProfile()
                                                .getAssignedSlots()
                                                .length));
    }

    @Test
    public void testSavepointStartPreconditionFailureKeepsJobRunning() throws Exception {
        long jobId = System.currentTimeMillis();
        startJob(jobId, BUSY_STREAM_CONF_PATH, false);

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        server.getCoordinatorService().getJobStatus(jobId)));

        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);
        awaitCheckpointCoordinatorsReady(jobMaster);
        setCheckpointCoordinatorsReady(jobMaster, false);

        PassiveCompletableFuture<Void> savepointFuture =
                server.getCoordinatorService().savePoint(jobId);

        // A too-early savepoint request should fail the request, but it has not started a
        // checkpoint. The job must stay RUNNING so the caller can retry after the checkpoint
        // coordinators report all tasks ready.
        Assertions.assertThrows(
                ExecutionException.class, () -> savepointFuture.get(120, TimeUnit.SECONDS));

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        server.getCoordinatorService().getJobStatus(jobId)));
        Assertions.assertNotNull(server.getCoordinatorService().getJobMaster(jobId));

        server.getCoordinatorService().cancelJob(jobId).join();
        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.CANCELED,
                                        server.getCoordinatorService().getJobStatus(jobId)));

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        0,
                                        server.getSlotService()
                                                .getWorkerProfile()
                                                .getAssignedSlots()
                                                .length));
    }

    @Test
    public void testSavepointStartPreconditionFailureCanBeRetriedAfterReady() throws Exception {
        long jobId = System.currentTimeMillis();
        startJob(jobId, RETRYABLE_SAVEPOINT_CONF_PATH, false);

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        server.getCoordinatorService().getJobStatus(jobId)));

        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);
        awaitCheckpointCoordinatorsReady(jobMaster);
        setCheckpointCoordinatorsReady(jobMaster, false);

        PassiveCompletableFuture<Void> savepointFuture =
                server.getCoordinatorService().savePoint(jobId);

        Assertions.assertThrows(
                ExecutionException.class, () -> savepointFuture.get(120, TimeUnit.SECONDS));

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        server.getCoordinatorService().getJobStatus(jobId)));

        setCheckpointCoordinatorsReady(jobMaster, true);
        awaitCheckpointCoordinatorsReady(jobMaster);

        PassiveCompletableFuture<Void> retrySavepointFuture =
                server.getCoordinatorService().savePoint(jobId);

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.SAVEPOINT_DONE,
                                        server.getCoordinatorService().getJobStatus(jobId)));
        retrySavepointFuture.get(120, TimeUnit.SECONDS);

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        0,
                                        server.getSlotService()
                                                .getWorkerProfile()
                                                .getAssignedSlots()
                                                .length));
    }

    @Test
    public void testSavepointFailureStopsWholeMultiPipelineJob() throws Exception {
        long jobId = System.currentTimeMillis();
        startJob(jobId, MULTI_PIPELINE_SAVEPOINT_PARTIAL_FAILURE_CONF_PATH, false);

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        server.getCoordinatorService().getJobStatus(jobId)));
        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);
        Assertions.assertEquals(2, jobMaster.getPhysicalPlan().getPipelineList().size());
        awaitCheckpointCoordinatorsReady(jobMaster);

        PassiveCompletableFuture<Void> savepointFuture =
                server.getCoordinatorService().savePoint(jobId);

        Assertions.assertThrows(
                ExecutionException.class, () -> savepointFuture.get(120, TimeUnit.SECONDS));

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(() -> assertFailedSavepointTerminalStatus(jobId));
        Assertions.assertNotEquals(
                JobStatus.RUNNING, server.getCoordinatorService().getJobStatus(jobId));
        Assertions.assertNotEquals(
                JobStatus.DOING_SAVEPOINT, server.getCoordinatorService().getJobStatus(jobId));
        jobMaster
                .getPhysicalPlan()
                .getPipelineList()
                .forEach(
                        subPlan ->
                                Assertions.assertTrue(
                                        subPlan.getPipelineState().isEndState(),
                                        "unexpected pipeline status "
                                                + subPlan.getPipelineState()));
        Assertions.assertTrue(
                jobMaster.getPhysicalPlan().getPipelineList().stream()
                        .map(
                                subPlan ->
                                        jobMaster
                                                .getCheckpointManager()
                                                .waitCheckpointCoordinatorComplete(
                                                        subPlan.getPipelineId())
                                                .join()
                                                .getCheckpointCoordinatorStatus())
                        .anyMatch(CheckpointCoordinatorStatus.SUSPEND::equals),
                "expected the successful pipeline checkpoint coordinator to reach SUSPEND");
        jobMaster
                .getPhysicalPlan()
                .getPipelineList()
                .forEach(
                        subPlan ->
                                Assertions.assertNotEquals(
                                        CheckpointCoordinatorStatus.RUNNING,
                                        jobMaster
                                                .getCheckpointManager()
                                                .waitCheckpointCoordinatorComplete(
                                                        subPlan.getPipelineId())
                                                .join()
                                                .getCheckpointCoordinatorStatus()));

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        0,
                                        server.getSlotService()
                                                .getWorkerProfile()
                                                .getAssignedSlots()
                                                .length));
    }

    private void setCheckpointCoordinatorsReady(JobMaster jobMaster, boolean ready) {
        jobMaster
                .getPhysicalPlan()
                .getPipelineList()
                .forEach(
                        subPlan ->
                                getCheckpointCoordinatorReadyFlag(jobMaster, subPlan).set(ready));
    }

    private void awaitCheckpointCoordinatorsReady(JobMaster jobMaster) {
        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                jobMaster
                                        .getPhysicalPlan()
                                        .getPipelineList()
                                        .forEach(
                                                subPlan -> {
                                                    AtomicBoolean isAllTaskReady =
                                                            getCheckpointCoordinatorReadyFlag(
                                                                    jobMaster, subPlan);
                                                    Assertions.assertTrue(
                                                            isAllTaskReady.get(),
                                                            "checkpoint coordinator not ready for pipeline "
                                                                    + subPlan.getPipelineId());
                                                }));
    }

    private AtomicBoolean getCheckpointCoordinatorReadyFlag(JobMaster jobMaster, SubPlan subPlan) {
        return (AtomicBoolean)
                ReflectionUtils.getField(
                                jobMaster
                                        .getCheckpointManager()
                                        .getCheckpointCoordinator(subPlan.getPipelineId()),
                                "isAllTaskReady")
                        .orElseThrow(() -> new AssertionError("isAllTaskReady field not found"));
    }

    private void assertFailedSavepointTerminalStatus(long jobId) {
        JobStatus status = server.getCoordinatorService().getJobStatus(jobId);
        Assertions.assertTrue(
                status == JobStatus.FAILED || status == JobStatus.CANCELED,
                "unexpected status " + status);
    }
}
