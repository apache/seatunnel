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
import org.apache.seatunnel.engine.common.job.JobStatusData;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.PendingJobInfo;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobHistoryService.JobState;
import org.apache.seatunnel.engine.server.master.JobHistoryService.PipelineStateData;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.JobCounter;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.map.IMap;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/** Exercises count aggregation on remote partitions, independently of the single-server job IT. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JobHistoryServiceJobCountTest {
    private final List<HazelcastInstance> members = new ArrayList<>();
    private final String clusterName = "job-count-test-" + UUID.randomUUID();
    private final Map<Long, JobMaster> runningMasters = new HashMap<>();
    private final Map<Long, PendingJobInfo> pendingJobs = new HashMap<>();
    private final List<IMap<?, ?>> maps = new ArrayList<>();
    private IMap<Object, Object> runningStates;
    private IMap<Long, JobState> finishedStates;
    private JobHistoryService history;
    private CoordinatorService coordinator;
    private int seedPort;

    @BeforeAll
    void startMembers() throws Exception {
        seedPort = freePort();
        members.add(Hazelcast.newHazelcastInstance(config(seedPort)));
        members.add(Hazelcast.newHazelcastInstance(config(freePort())));
        members.add(Hazelcast.newHazelcastInstance(config(freePort())));
        awaitSafeCluster(3);
    }

    @AfterAll
    void stopMembers() {
        members.forEach(member -> member.getLifecycleService().terminate());
    }

    @BeforeEach
    void createHistory() throws Exception {
        runningMasters.clear();
        pendingJobs.clear();
        runningStates = newMap();
        finishedStates = newMap();
        history = history(finishedStates);
        coordinator = Mockito.mock(CoordinatorService.class, Mockito.CALLS_REAL_METHODS);
        Field field = CoordinatorService.class.getDeclaredField("jobHistoryService");
        field.setAccessible(true);
        field.set(coordinator, history);
    }

    @AfterEach
    void destroyMaps() {
        maps.forEach(IMap::destroy);
        maps.clear();
    }

    @Test
    void countsAllStatusesAcrossMembersWithoutChangingHistory() {
        for (long id = 0; id < JobStatus.values().length * 10; id++) {
            finishedStates.put(
                    id, state(id, JobStatus.values()[(int) (id % JobStatus.values().length)]));
        }
        assertTrue(
                finishedStates.keySet().stream()
                        .anyMatch(
                                id ->
                                        !members.get(0)
                                                .getPartitionService()
                                                .getPartition(id)
                                                .getOwner()
                                                .localMember()));
        Map<Long, JobState> before = finishedStates.getAll(finishedStates.keySet());
        assertMatchesListing();
        assertEquals(before, finishedStates.getAll(finishedStates.keySet()));
        assertEquals(10L, coordinator.getJobCountMetrics().getFinishedJobCount());
    }

    @Test
    void activeSnapshotsSuppressRetainedAttemptsAndKeepOverlapBehavior() {
        runningMasters.put(1L, master(1L, JobStatus.FAILED));
        runningStates.put(1L, JobStatus.RUNNING);
        pendingJobs.put(2L, pending(2L));
        runningMasters.put(3L, master(3L, JobStatus.RUNNING));
        pendingJobs.put(3L, pending(3L));
        runningMasters.put(4L, master(4L, JobStatus.INITIALIZING));
        runningMasters.put(5L, master(5L, JobStatus.FAILING));
        for (long id = 1; id <= 5; id++) {
            finishedStates.put(id, state(id, JobStatus.FAILED));
        }
        // The existing listing suppresses by the value's job ID, not the IMap key.
        finishedStates.put(99L, state(1L, JobStatus.FAILED));
        finishedStates.put(6L, state(6L, JobStatus.FINISHED));
        assertMatchesListing();
        assertEquals(new JobCounter(0, 2, 0, 2, 1, 0, 0, 0, 1), coordinator.getJobCountMetrics());

        runningMasters.remove(1L);
        pendingJobs.remove(2L);
        assertMatchesListing();
        assertEquals(3L, coordinator.getJobCountMetrics().getFailedJobCount());
    }

    @Test
    void countsEmptyHistoryAndUninitializedCoordinator() throws Exception {
        assertMatchesListing();
        assertEquals(new JobCounter(0, 0, 0, 0, 0, 0, 0, 0, 0), coordinator.getJobCountMetrics());
        Field field = CoordinatorService.class.getDeclaredField("jobHistoryService");
        field.setAccessible(true);
        field.set(coordinator, null);
        assertEquals(new JobCounter(0, 0, 0, 0, 0, 0, 0, 0, 0), coordinator.getJobCountMetrics());
    }

    @Test
    void scrapesDoNotExtendRetainedRecordExpiry() {
        finishedStates.put(1L, state(1L, JobStatus.FINISHED), 1, TimeUnit.SECONDS);
        finishedStates.put(2L, state(2L, JobStatus.CANCELED));
        await().atMost(20, TimeUnit.SECONDS)
                .pollInterval(50, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            coordinator.getJobCountMetrics();
                            assertFalse(finishedStates.containsKey(1L));
                        });
        assertMatchesListing();
        assertEquals(new JobCounter(0, 0, 0, 0, 0, 0, 0, 1, 0), coordinator.getJobCountMetrics());
    }

    @Test
    void retainsCountsAfterMemberLossAndPartitionMigration() throws Exception {
        for (long id = 0; id < JobStatus.values().length * 10; id++) {
            finishedStates.put(
                    id, state(id, JobStatus.values()[(int) (id % JobStatus.values().length)]));
        }
        awaitSafeCluster(3);
        assertFalse(members.get(2).getMap(finishedStates.getName()).localKeySet().isEmpty());
        JobCounter expected = coordinator.getJobCountMetrics();
        try {
            members.get(2).getLifecycleService().terminate();
            awaitSafeCluster(2);
            assertEquals(JobStatus.values().length * 10, finishedStates.size());
            assertMatchesListing();
            assertEquals(expected, coordinator.getJobCountMetrics());
        } finally {
            members.set(2, Hazelcast.newHazelcastInstance(config(freePort())));
            awaitSafeCluster(3);
        }
        assertMatchesListing();
        assertEquals(expected, coordinator.getJobCountMetrics());
    }

    @Test
    void queryFailurePropagatesInsteadOfReportingZeroCounts() {
        IMap<Long, JobState> failedMap = Mockito.spy(finishedStates);
        Mockito.doThrow(new IllegalStateException("count query failed"))
                .when(failedMap)
                .aggregate(any());
        JobHistoryService failedHistory = history(failedMap);
        assertThrows(IllegalStateException.class, failedHistory::getJobStatusCounts);
    }

    private void assertMatchesListing() {
        Map<JobStatus, Long> expected = new EnumMap<>(JobStatus.class);
        for (JobStatusData state : history.getJobStatusData()) {
            expected.merge(state.getJobStatus(), 1L, Long::sum);
        }
        assertEquals(expected, history.getJobStatusCounts());
        assertEquals(
                new JobCounter(
                        expected.getOrDefault(JobStatus.CREATED, 0L),
                        expected.getOrDefault(JobStatus.PENDING, 0L),
                        expected.getOrDefault(JobStatus.SCHEDULED, 0L),
                        expected.getOrDefault(JobStatus.RUNNING, 0L),
                        expected.getOrDefault(JobStatus.FAILING, 0L),
                        expected.getOrDefault(JobStatus.FAILED, 0L),
                        expected.getOrDefault(JobStatus.CANCELING, 0L),
                        expected.getOrDefault(JobStatus.CANCELED, 0L),
                        expected.getOrDefault(JobStatus.FINISHED, 0L)),
                coordinator.getJobCountMetrics());
    }

    private JobMaster master(long id, JobStatus fallback) {
        JobMaster master = Mockito.mock(JobMaster.class);
        JobImmutableInformation information = Mockito.mock(JobImmutableInformation.class);
        when(information.getJobId()).thenReturn(id);
        when(information.getJobName()).thenReturn("job-" + id);
        when(information.getCreateTime()).thenReturn(1L);
        when(master.getJobImmutableInformation()).thenReturn(information);
        when(master.getJobStatus()).thenReturn(fallback);
        return master;
    }

    private PendingJobInfo pending(long id) {
        PendingJobInfo pending = Mockito.mock(PendingJobInfo.class);
        JobMaster master = master(id, JobStatus.PENDING);
        when(pending.getJobMaster()).thenReturn(master);
        return pending;
    }

    private JobState state(long id, JobStatus status) {
        return new JobState(
                id,
                "job-" + id,
                status,
                1L,
                2L,
                3L,
                Collections.singletonMap(
                        new PipelineLocation(id, 1),
                        new PipelineStateData(
                                PipelineStatus.FINISHED,
                                Collections.singletonMap(
                                        new TaskGroupLocation(id, 1, 1), ExecutionState.FINISHED))),
                "retained diagnostic");
    }

    private JobHistoryService history(IMap<Long, JobState> finished) {
        HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) members.get(0);
        return new JobHistoryService(
                proxy.getOriginal().node.nodeEngine,
                runningStates,
                members.get(0).getLoggingService().getLogger(JobHistoryServiceJobCountTest.class),
                pendingJobs,
                runningMasters,
                finished,
                newMap(),
                newMap(),
                1440);
    }

    private <K, V> IMap<K, V> newMap() {
        IMap<K, V> map = members.get(0).getMap("job-count-" + UUID.randomUUID());
        maps.add(map);
        return map;
    }

    private void awaitSafeCluster(int size) {
        await().atMost(60, TimeUnit.SECONDS)
                .until(
                        () ->
                                members.get(0).getCluster().getMembers().size() == size
                                        && members.get(0).getPartitionService().isClusterSafe());
    }

    private Config config(int port) {
        Config config = new Config().setClusterName(clusterName);
        config.setProperty("hazelcast.logging.type", "none");
        config.setProperty("hazelcast.operation.thread.count", "2");
        config.setProperty("hazelcast.operation.generic.thread.count", "2");
        config.setProperty("hazelcast.io.input.thread.count", "1");
        config.setProperty("hazelcast.io.output.thread.count", "1");
        config.setProperty("hazelcast.query.thread.count", "2");
        config.setProperty("hazelcast.heartbeat.interval.seconds", "1");
        config.setProperty("hazelcast.max.no.heartbeat.seconds", "5");
        config.getJetConfig().setEnabled(false);
        config.getNetworkConfig().setPort(port).setPortAutoIncrement(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1:" + seedPort);
        return config;
    }

    private int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"))) {
            return socket.getLocalPort();
        }
    }
}
