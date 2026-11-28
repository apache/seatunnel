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

package org.apache.seatunnel.engine.e2e.deploycancel;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.exception.TaskGroupContextNotFoundException;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end guard for cancelling a Zeta job while one of its task groups is still being deployed.
 *
 * <p>The window under test sits between two lifecycle steps that existing E2E coverage does not
 * combine: the pipeline already owns its fixed slots and the worker already published the task
 * group's execution context, but the task's sink writer has not been created yet, so the task has
 * not reported {@code READY_START} and the job has produced no data. Cancelling there exercises the
 * engine's teardown of a deployment whose task never started: the job must reach {@code CANCELED}
 * within a bounded time instead of wedging in {@code CANCELING}, the task group must stop being
 * actively deployed on the worker, the terminal state must stay resolvable after the {@link
 * JobMaster} is released, every fixed slice must return to the pool, and the freed capacity must
 * accept a new job that then runs normally.
 *
 * <p>The contract deliberately pins that such a cancel resolves on its own: the parked restore
 * function is held for longer than the cancel bound, so the job can only pass if cancellation
 * neither waits for the deployment nor depends on it finishing, and a lost task-group completion
 * signal leaves the job non-terminal past the bound. Once the cancel resolved, releasing the parked
 * function must not resurrect the job, its task group or its slices either.
 *
 * <p>Existing coverage stops short of that boundary. {@code JobClientJobProxyIT} cancels only after
 * the job is {@code RUNNING} and its sink is already writing. {@code
 * SplitClusterFaultToleranceIT#testStreamJobCancelResolvesWhenWorkerCrashesBeforeCancelAck} pins
 * the cancel acknowledgement path when the worker disappears, not a cancel that reaches a
 * deployed-but-not-started task. {@code SavepointRestoreIT}, {@code CheckpointRestoreWithStopIT}
 * and the pending-job cancel scenarios all cancel work that is either already running or has not
 * been deployed at all.
 *
 * <p>Determinism: {@link DeploymentHoldSinkWriter} parks its own construction, which the engine
 * performs from the action's restore function, and reports the parked call from that exact place.
 * The test therefore waits on the deployment instead of guessing a sleep, and the "no row reached
 * the sink" assertion proves the task never started before the cancel.
 */
@Slf4j
public class CancelDuringDeploymentIT {

    private static final String CLUSTER_NAME = "CancelDuringDeploymentIT";
    private static final String JOB_CONFIG_FILE = "stream_fake_to_deployment_hold_sink.conf";

    /** Fixed slice count of the single hybrid node. */
    private static final int SLOT_NUM = 6;

    /**
     * Bound for a submitted job to reach the parked writer creation. It exceeds a normal cold
     * deployment by a wide margin so CI slowness cannot be mistaken for a broken fixture.
     */
    private static final long DEPLOYMENT_ENTER_TIMEOUT_SECONDS = 180;

    /**
     * Bound for a cancel accepted inside the deployment window to resolve the job. It stays far
     * below the gate's own hold, so the job can only pass if the cancel never waited for the parked
     * deployment.
     */
    private static final long CANCEL_TIMEOUT_SECONDS = 60;

    /** Window in which a released deployment must stay dead. */
    private static final long NO_RESURRECTION_WINDOW_SECONDS = 8;

    private HazelcastInstanceImpl node;
    private SeaTunnelServer server;
    private SeaTunnelClient engineClient;
    private SeaTunnelConfig seaTunnelConfig;

    @BeforeEach
    public void setUp() {
        seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(TestUtils.getClusterName(CLUSTER_NAME));
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(SLOT_NUM);
        seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);

        node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
        server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);

        Common.setDeployMode(DeployMode.CLIENT);
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(CLUSTER_NAME));
        engineClient = new SeaTunnelClient(clientConfig);

        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        1,
                                        coordinatorService()
                                                .getResourceManager()
                                                .workerCount(Collections.emptyMap()),
                                        "The hybrid node must register as a worker"));
    }

    @AfterEach
    public void tearDown() {
        DeploymentHoldGate.release();
        if (engineClient != null) {
            engineClient.close();
        }
        if (node != null) {
            node.shutdown();
        }
    }

    @Test
    public void testCancelWhileTheTaskIsStillDeploying() throws Exception {
        DeploymentHoldGate.reset();

        ClientJobProxy jobProxy = submitJob("cancel-during-deployment");
        long jobId = jobProxy.getJobId();
        log.info("Submitted job {} and waiting for its deployment window", jobId);

        Assertions.assertTrue(
                DeploymentHoldGate.openEntered()
                        .await(DEPLOYMENT_ENTER_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                String.format(
                        "Job %s never reached the parked sink writer creation; the task deployment window was not entered",
                        jobId));
        Assertions.assertEquals(
                1,
                DeploymentHoldGate.openAttempts(),
                "The sink writer must be created exactly once");
        Assertions.assertEquals(
                0,
                DeploymentHoldGate.writtenRows(),
                "No row may reach the sink before the deployment completes");
        JobStatus beforeCancel = jobProxy.getJobStatus();
        Assertions.assertFalse(
                beforeCancel.isEndState(),
                String.format(
                        "Job %s must not be terminal while it is still deploying, but was %s",
                        jobId, beforeCancel));

        List<TaskGroupLocation> taskGroups = taskGroupLocations(jobId);
        Assertions.assertFalse(
                taskGroups.isEmpty(), "The deploying job must expose its task groups");
        assertSlotsOwnedByJob(jobId);

        log.info("Cancelling job {} while its deployment is parked at the writer creation", jobId);
        jobProxy.cancelJob();

        JobResult jobResult = awaitJobResult(jobProxy);
        Assertions.assertEquals(
                JobStatus.CANCELED,
                jobResult.getStatus(),
                "A cancel accepted during deployment must end the job CANCELED");
        Assertions.assertFalse(
                DeploymentHoldGate.timedOut(),
                "The parked deployment must still be in flight when the cancel resolves, not finished on its own");
        Assertions.assertEquals(
                1,
                DeploymentHoldGate.openAttempts(),
                "The cancel must not create another writer for the cancelled deployment");
        Assertions.assertEquals(
                0,
                DeploymentHoldGate.writtenRows(),
                "The cancelled deployment must not have produced any row");

        awaitJobMasterRelease(jobId);
        Assertions.assertEquals(
                JobStatus.CANCELED,
                coordinatorService().getJobStatus(jobId),
                "The terminal state must stay resolvable after the JobMaster is released");
        awaitSlotPoolReleased("the cancelled deployment");
        awaitNoActiveTaskGroup(taskGroups);

        // Complete the abandoned restore function. A deployment that finished after the cancel must
        // not revive the job, its task groups or its slices.
        log.info("Releasing the parked deployment of cancelled job {}", jobId);
        DeploymentHoldGate.release();
        Awaitility.await()
                .during(NO_RESURRECTION_WINDOW_SECONDS, TimeUnit.SECONDS)
                .atMost(NO_RESURRECTION_WINDOW_SECONDS + 20, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JobStatus.CANCELED,
                                    coordinatorService().getJobStatus(jobId),
                                    "A deployment released after the cancel must not revive the job");
                            Assertions.assertNull(
                                    coordinatorService().getJobMaster(jobId),
                                    "A deployment released after the cancel must not rebuild the JobMaster");
                            Assertions.assertEquals(
                                    0,
                                    currentAssignedSlots().length,
                                    "A deployment released after the cancel must not re-acquire slices");
                            for (TaskGroupLocation taskGroup : taskGroups) {
                                Assertions.assertFalse(
                                        isTaskGroupActive(taskGroup),
                                        "A deployment released after the cancel must not redeploy its task group");
                            }
                        });

        // The fixed capacity freed by the cancelled deployment must run a new job normally.
        ClientJobProxy followUp = submitJob("follow-up-after-cancel");
        long followUpJobId = followUp.getJobId();
        log.info("Submitted follow-up job {} on the capacity freed by the cancel", followUpJobId);
        Awaitility.await()
                .atMost(DEPLOYMENT_ENTER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    2,
                                    DeploymentHoldGate.openAttempts(),
                                    "The follow-up job must deploy its own sink writer");
                            Assertions.assertEquals(
                                    JobStatus.RUNNING,
                                    followUp.getJobStatus(),
                                    "The follow-up job must reach RUNNING on the freed slices");
                            Assertions.assertTrue(
                                    DeploymentHoldGate.writtenRows() > 0,
                                    "The follow-up job must stream rows through the sink");
                        });
        SlotProfile[] followUpSlots = currentAssignedSlots();
        Assertions.assertTrue(
                followUpSlots.length > 0, "The follow-up job must own fixed slices while running");
        for (SlotProfile slot : followUpSlots) {
            Assertions.assertEquals(
                    followUpJobId,
                    slot.getOwnerJobID(),
                    "Every slice must be owned by the follow-up job");
        }

        followUp.cancelJob();
        JobResult followUpResult = awaitJobResult(followUp);
        Assertions.assertEquals(
                JobStatus.CANCELED, followUpResult.getStatus(), "The follow-up job must cancel");
        awaitSlotPoolReleased("the follow-up job");
        awaitJobMasterRelease(followUpJobId);
    }

    private ClientJobProxy submitJob(String name) throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(name);
        ClientJobExecutionEnvironment environment =
                engineClient.createExecutionContext(
                        TestUtils.getResource(JOB_CONFIG_FILE), jobConfig, seaTunnelConfig);
        try {
            return environment.execute();
        } catch (ExecutionException e) {
            throw new IllegalStateException(String.format("Failed to submit job %s", name), e);
        }
    }

    private List<TaskGroupLocation> taskGroupLocations(long jobId) {
        JobMaster jobMaster = coordinatorService().getJobMaster(jobId);
        Assertions.assertNotNull(
                jobMaster, String.format("Job %s must have a JobMaster while deploying", jobId));
        List<TaskGroupLocation> taskGroups = new ArrayList<>();
        for (SubPlan subPlan : jobMaster.getPhysicalPlan().getPipelineList()) {
            subPlan.getCoordinatorVertexList()
                    .forEach(vertex -> taskGroups.add(vertex.getTaskGroupLocation()));
            subPlan.getPhysicalVertexList()
                    .forEach(vertex -> taskGroups.add(vertex.getTaskGroupLocation()));
        }
        return taskGroups;
    }

    private void assertSlotsOwnedByJob(long jobId) {
        Awaitility.await()
                .atMost(DEPLOYMENT_ENTER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            SlotProfile[] assignedSlots = currentAssignedSlots();
                            Assertions.assertTrue(
                                    assignedSlots.length > 0,
                                    "The deploying job must own fixed slices");
                            for (SlotProfile slot : assignedSlots) {
                                Assertions.assertEquals(
                                        jobId,
                                        slot.getOwnerJobID(),
                                        String.format(
                                                "Slice %s must be owned by the deploying job %s",
                                                slot.getSlotID(), jobId));
                            }
                        });
    }

    private JobResult awaitJobResult(ClientJobProxy jobProxy) {
        CompletableFuture<JobResult> jobResultFuture =
                CompletableFuture.supplyAsync(jobProxy::waitForJobCompleteV2);
        Awaitility.await()
                .atMost(CANCEL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        jobResultFuture.isDone(),
                                        String.format(
                                                "The cancelled job did not reach a terminal state within %s seconds",
                                                CANCEL_TIMEOUT_SECONDS)));
        try {
            return jobResultFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the job result", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Waiting for the job result failed", e);
        }
    }

    private void awaitJobMasterRelease(long jobId) {
        Awaitility.await()
                .atMost(CANCEL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertNull(
                                        coordinatorService().getJobMaster(jobId),
                                        String.format(
                                                "The JobMaster of cancelled job %s must be released",
                                                jobId)));
    }

    private void awaitSlotPoolReleased(String phase) {
        Awaitility.await()
                .atMost(CANCEL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        0,
                                        currentAssignedSlots().length,
                                        String.format(
                                                "Every fixed slice must return to the pool after %s",
                                                phase)));
    }

    private void awaitNoActiveTaskGroup(List<TaskGroupLocation> taskGroups) {
        Awaitility.await()
                .atMost(CANCEL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            for (TaskGroupLocation taskGroup : taskGroups) {
                                Assertions.assertFalse(
                                        isTaskGroupActive(taskGroup),
                                        String.format(
                                                "Task group %s must not stay actively deployed after the cancel",
                                                taskGroup));
                            }
                        });
    }

    /**
     * Reports whether the task group still has an active worker-side execution context. The engine
     * throws instead of returning null when a location is gone, which is the expected state here.
     */
    private boolean isTaskGroupActive(TaskGroupLocation taskGroup) {
        try {
            server.getTaskExecutionService().getActiveExecutionContext(taskGroup);
            return true;
        } catch (TaskGroupContextNotFoundException e) {
            return false;
        }
    }

    private SlotProfile[] currentAssignedSlots() {
        SlotProfile[] assigned = server.getSlotService().getWorkerProfile().getAssignedSlots();
        return assigned == null ? new SlotProfile[0] : assigned;
    }

    private CoordinatorService coordinatorService() {
        return server.getCoordinatorService();
    }
}
