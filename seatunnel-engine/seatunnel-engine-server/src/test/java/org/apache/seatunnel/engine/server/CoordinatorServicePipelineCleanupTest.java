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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.cleanup.PipelineCleanupRecord;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.map.IMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

class CoordinatorServicePipelineCleanupTest extends AbstractSeaTunnelServerTest {

    @Test
    void testCleanupRemovesMetricsAndRecordWhenNoTaskGroups() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        awaitCoordinatorActive(coordinatorService);

        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        PipelineLocation otherPipelineLocation = new PipelineLocation(jobId + 1, 1);

        upsertMetricsForPipeline(pipelineLocation);
        upsertMetricsForPipeline(otherPipelineLocation);
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));
        Assertions.assertTrue(hasMetricsForPipeline(otherPipelineLocation));

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(pipelineLocation, PipelineStatus.FINISHED);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        pendingCleanupIMap.put(
                pipelineLocation,
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.FINISHED,
                        false,
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0));

        coordinatorService.runPendingPipelineCleanupOnce();

        Assertions.assertFalse(hasMetricsForPipeline(pipelineLocation));
        Assertions.assertTrue(hasMetricsForPipeline(otherPipelineLocation));
        Assertions.assertFalse(pendingCleanupIMap.containsKey(pipelineLocation));
    }

    @Test
    void testSkipCleanupWhenPipelineNotEndState() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        awaitCoordinatorActive(coordinatorService);

        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);

        upsertMetricsForPipeline(pipelineLocation);
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(pipelineLocation, PipelineStatus.RUNNING);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        PipelineCleanupRecord record =
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.FINISHED,
                        false,
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0);
        pendingCleanupIMap.put(pipelineLocation, record);

        coordinatorService.runPendingPipelineCleanupOnce();

        PipelineCleanupRecord after = pendingCleanupIMap.get(pipelineLocation);
        Assertions.assertNotNull(after);
        Assertions.assertEquals(0, after.getAttemptCount());
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));
    }

    @Test
    void testRemoveRecordWhenShouldCleanupIsFalse() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        awaitCoordinatorActive(coordinatorService);

        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        upsertMetricsForPipeline(pipelineLocation);
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(pipelineLocation, PipelineStatus.FINISHED);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        pendingCleanupIMap.put(
                pipelineLocation,
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.FINISHED,
                        true,
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0));

        coordinatorService.runPendingPipelineCleanupOnce();

        Assertions.assertFalse(pendingCleanupIMap.containsKey(pipelineLocation));
        Assertions.assertTrue(
                hasMetricsForPipeline(pipelineLocation),
                "Should not clean metrics when record is removed due to shouldCleanup=false");
    }

    @Test
    void testCleanupUpdatesRecordAndKeepsItWhenTaskGroupCannotBeCleaned() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        awaitCoordinatorActive(coordinatorService);

        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        upsertMetricsForPipeline(pipelineLocation);
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(pipelineLocation, PipelineStatus.CANCELED);

        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);
        Map<TaskGroupLocation, Address> taskGroups = new HashMap<>();
        taskGroups.put(taskGroupLocation, null);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        pendingCleanupIMap.put(
                pipelineLocation,
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.CANCELED,
                        false,
                        taskGroups,
                        new HashSet<>(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0));

        coordinatorService.runPendingPipelineCleanupOnce();

        PipelineCleanupRecord updated = pendingCleanupIMap.get(pipelineLocation);
        Assertions.assertNotNull(updated);
        Assertions.assertEquals(1, updated.getAttemptCount());
        Assertions.assertTrue(updated.isMetricsImapCleaned());
        Assertions.assertFalse(updated.isCleaned());
        Assertions.assertFalse(updated.getCleanedTaskGroups().contains(taskGroupLocation));
        Assertions.assertFalse(hasMetricsForPipeline(pipelineLocation));
    }

    @Test
    void testCleanupRemovesRecordWhenAllTaskGroupsCleaned() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        awaitCoordinatorActive(coordinatorService);

        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        upsertMetricsForPipeline(pipelineLocation);
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(pipelineLocation, PipelineStatus.CANCELED);

        Address localAddress = instance.getCluster().getLocalMember().getAddress();
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);
        Map<TaskGroupLocation, Address> taskGroups = new HashMap<>();
        taskGroups.put(taskGroupLocation, localAddress);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        pendingCleanupIMap.put(
                pipelineLocation,
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.CANCELED,
                        false,
                        taskGroups,
                        new HashSet<>(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0));

        coordinatorService.runPendingPipelineCleanupOnce();

        Assertions.assertFalse(hasMetricsForPipeline(pipelineLocation));
        Assertions.assertFalse(pendingCleanupIMap.containsKey(pipelineLocation));
    }

    private void upsertMetricsForPipeline(PipelineLocation pipelineLocation) {
        TaskGroupLocation taskGroupLocation =
                new TaskGroupLocation(
                        pipelineLocation.getJobId(), pipelineLocation.getPipelineId(), 1L);
        TaskLocation taskLocation = new TaskLocation(taskGroupLocation, 0, 0);

        Map<TaskLocation, SeaTunnelMetricsContext> local = new HashMap<>();
        local.put(taskLocation, new SeaTunnelMetricsContext());
        server.updateMetrics(local);
    }

    private boolean hasMetricsForPipeline(PipelineLocation pipelineLocation) {
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> metricsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_METRICS);
        return metricsIMap.entrySet().stream()
                .flatMap(entry -> entry.getValue().keySet().stream())
                .anyMatch(
                        taskLocation ->
                                pipelineLocation.equals(
                                        taskLocation.getTaskGroupLocation().getPipelineLocation()));
    }

    private void awaitCoordinatorActive(CoordinatorService coordinatorService) {
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> Assertions.assertTrue(coordinatorService.isCoordinatorActive()));
    }
}
