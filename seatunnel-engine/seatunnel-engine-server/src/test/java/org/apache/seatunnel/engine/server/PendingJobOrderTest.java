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
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.execution.PendingJobInfo;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.core.classloader.DefaultClassLoaderService.SKIP_CHECK_JAR;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PendingJobOrderTest {
    @Test
    void legacyJobsFollowRecordedAdmissionsWithDeterministicTies() {
        JobInfo earlierAdmission = new JobInfo(200L, null);
        earlierAdmission.setEnqueueSequence(3L);
        JobInfo laterAdmission = new JobInfo(100L, null);
        laterAdmission.setEnqueueSequence(9L);
        List<Map.Entry<Long, JobInfo>> jobs =
                new ArrayList<>(
                        Arrays.asList(
                                new AbstractMap.SimpleEntry<>(5L, new JobInfo(20L, null)),
                                new AbstractMap.SimpleEntry<>(2L, laterAdmission),
                                new AbstractMap.SimpleEntry<>(4L, new JobInfo(20L, null)),
                                new AbstractMap.SimpleEntry<>(1L, earlierAdmission),
                                new AbstractMap.SimpleEntry<>(3L, new JobInfo(10L, null))));
        CoordinatorService.orderJobsForRestore(jobs);
        assertEquals(
                Arrays.asList(1L, 2L, 3L, 4L, 5L),
                jobs.stream().map(Map.Entry::getKey).collect(Collectors.toList()));
    }

    @Test
    @SetEnvironmentVariable(key = SKIP_CHECK_JAR, value = "true")
    void fiftyWaitingJobsKeepTheirOrderAcrossMasterTermination() throws Exception {
        String clusterName = TestUtils.getClusterName("pending-order-" + UUID.randomUUID());
        int port = TestUtils.getAvailablePort(3);
        List<HazelcastInstanceImpl> instances = new ArrayList<>();
        try {
            HazelcastInstanceImpl first = startMember(clusterName, port);
            instances.add(first);
            CoordinatorService original = coordinator(first);
            await().atMost(60, TimeUnit.SECONDS).until(original::isCoordinatorActive);
            HazelcastInstanceImpl second = startMember(clusterName, port);
            instances.add(second);
            await().atMost(60, TimeUnit.SECONDS)
                    .until(() -> first.getCluster().getMembers().size() == 2);

            for (int i = 0; i < 50; i++) {
                submitWaitingJob(first, original);
            }
            List<Long> expected = pendingOrder(original);
            assertEquals(50, expected.size());
            IMap<Long, JobInfo> metadata = first.getMap(Constant.IMAP_RUNNING_JOB_INFO);
            long previous = 0;
            for (Long id : expected) {
                assertEquals(JobStatus.PENDING, original.getJobStatus(id));
                long sequence = metadata.get(id).getEnqueueSequence();
                assertTrue(sequence > previous);
                previous = sequence;
            }
            await().atMost(60, TimeUnit.SECONDS)
                    .until(() -> first.getPartitionService().isClusterSafe());
            first.getLifecycleService().terminate();

            CoordinatorService restored = coordinator(second);
            awaitRestoredOrder(restored, expected);
            // Only the queue head may build a physical plan while every job lacks slots.
            assertTrue(initializedCount(restored) <= 1);
            for (Long id : expected) {
                assertEquals(JobStatus.PENDING, restored.getJobStatus(id));
                assertNotNull(restored.getJobHistoryService().getJobDetailState(id));
                assertFalse(restored.waitForJobComplete(id).isDone());
            }
            assertEquals(50, restored.getJobHistoryService().getJobStatusData().size());
            restored.getPendingJobs(Collections.emptyMap(), null, 50);
            assertTrue(initializedCount(restored) <= 1);

            // New submissions append behind the recovered backlog.
            long newId = submitWaitingJob(second, restored);
            expected.add(newId);
            assertEquals(expected, pendingOrder(restored));
            assertTrue(
                    second.<Long, JobInfo>getMap(Constant.IMAP_RUNNING_JOB_INFO)
                                    .get(newId)
                                    .getEnqueueSequence()
                            > previous);

            // Cancel an unmaterialized job. Its neighbours retain their positions.
            long canceledId = expected.remove(25);
            PassiveCompletableFuture<JobResult> canceledCompletion =
                    restored.waitForJobComplete(canceledId);
            restored.cancelJob(canceledId).get(60, TimeUnit.SECONDS);
            assertEquals(
                    JobStatus.CANCELED, canceledCompletion.get(60, TimeUnit.SECONDS).getStatus());
            await().atMost(60, TimeUnit.SECONDS)
                    .until(() -> restored.getJobStatus(canceledId) == JobStatus.CANCELED);
            assertEquals(expected, pendingOrder(restored));

            long stoppedId = expected.remove(24);
            PassiveCompletableFuture<JobResult> stoppedCompletion =
                    restored.waitForJobComplete(stoppedId);
            restored.stopJob(stoppedId).get(60, TimeUnit.SECONDS);
            assertEquals(
                    JobStatus.CANCELED, stoppedCompletion.get(60, TimeUnit.SECONDS).getStatus());
            assertEquals(expected, pendingOrder(restored));

            // A second failover must preserve the assigned order and not resurrect cancellation.
            HazelcastInstanceImpl third = startMember(clusterName, port);
            instances.add(third);
            await().atMost(60, TimeUnit.SECONDS)
                    .until(() -> second.getCluster().getMembers().size() == 2);
            await().atMost(60, TimeUnit.SECONDS)
                    .until(() -> second.getPartitionService().isClusterSafe());
            second.getLifecycleService().terminate();
            CoordinatorService restoredAgain = coordinator(third);
            awaitRestoredOrder(restoredAgain, expected);
            assertEquals(JobStatus.CANCELED, restoredAgain.getJobStatus(canceledId));
            assertEquals(JobStatus.CANCELED, restoredAgain.getJobStatus(stoppedId));
            assertTrue(initializedCount(restoredAgain) <= 1);
        } finally {
            for (HazelcastInstanceImpl instance : instances) {
                if (instance.getLifecycleService().isRunning()) {
                    instance.getLifecycleService().terminate();
                }
            }
        }
    }

    private HazelcastInstanceImpl startMember(String clusterName, int port) {
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        config.getHazelcastConfig().setClusterName(clusterName);
        config.getHazelcastConfig().getNetworkConfig().setPort(port).setPortCount(3);
        config.getHazelcastConfig()
                .getNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .setMembers(
                        Arrays.asList(
                                "127.0.0.1:" + port,
                                "127.0.0.1:" + (port + 1),
                                "127.0.0.1:" + (port + 2)));
        config.getHazelcastConfig().setProperty("hazelcast.tcp.join.port.try.count", "3");
        config.getHazelcastConfig().setProperty("hazelcast.partition.count", "31");
        config.getHazelcastConfig().setProperty("hazelcast.operation.thread.count", "2");
        config.getHazelcastConfig().setProperty("hazelcast.operation.generic.thread.count", "2");
        config.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);
        config.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        config.getEngineConfig().getSlotServiceConfig().setSlotNum(1);
        return SeaTunnelServerStarter.createHazelcastInstance(config);
    }

    private long submitWaitingJob(HazelcastInstanceImpl instance, CoordinatorService coordinator)
            throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        LogicalDag dag =
                TestUtils.createTestLogicalPlan(
                        "batch_fake_to_console.conf", "waiting-" + jobId, jobId);
        dag.getLogicalVertexMap()
                .values()
                .forEach(
                        vertex -> {
                            vertex.setParallelism(3);
                            vertex.getAction().setParallelism(3);
                        });
        JobImmutableInformation information =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        instance.getSerializationService(),
                        dag,
                        Collections.emptyList(),
                        Collections.emptyList());
        coordinator
                .submitJob(jobId, instance.getSerializationService().toData(information), false)
                .get(60, TimeUnit.SECONDS);
        return jobId;
    }

    private CoordinatorService coordinator(HazelcastInstanceImpl instance) {
        SeaTunnelServer server =
                instance.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return await().atMost(60, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(server::getCoordinatorService, CoordinatorService::isCoordinatorActive);
    }

    private void awaitRestoredOrder(CoordinatorService coordinator, List<Long> expected) {
        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertTrue(coordinator.isCoordinatorActive());
                            assertEquals(expected, pendingOrder(coordinator));
                        });
    }

    private long initializedCount(CoordinatorService coordinator) {
        return coordinator.getPendingJobQueue().getJobIdMap().values().stream()
                .filter(info -> info.getInitializedJobMaster() != null)
                .count();
    }

    @SuppressWarnings("unchecked")
    private List<Long> pendingOrder(CoordinatorService coordinator) {
        // Observe the actual FIFO, not the unordered job-id index or the proposed sort algorithm.
        Collection<PendingJobInfo> queue =
                (Collection<PendingJobInfo>)
                        ReflectionUtils.getField(coordinator.getPendingJobQueue(), "queue").get();
        return queue.stream().map(PendingJobInfo::getJobId).collect(Collectors.toList());
    }
}
