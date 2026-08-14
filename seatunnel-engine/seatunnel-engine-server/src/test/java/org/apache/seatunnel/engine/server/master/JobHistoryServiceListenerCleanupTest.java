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

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Regression test for the finished-job IMap listener leak across master role switches.
 *
 * <p>{@link JobHistoryService} registers expiration listeners on the cluster-wide finished-job
 * IMaps in its constructor, and {@link CoordinatorService} creates a new instance every time a node
 * becomes the active master. Before this fix there was no deregistration path, so every
 * active-master transition leaked the previous listeners, kept the old service instance reachable
 * and duplicated finished-job expiration side effects such as clean-log operation fan-out. This
 * test verifies that {@link JobHistoryService#close()} removes exactly the listeners registered by
 * the closed instance and that {@link CoordinatorService#clearCoordinatorService()} performs that
 * cleanup when a node leaves the active master role.
 */
public class JobHistoryServiceListenerCleanupTest extends AbstractSeaTunnelServerTest {

    /**
     * Verifies that close() deregisters the three expiration listeners registered by the
     * constructor, and that repeated create/close cycles, which emulate repeated active-master
     * transitions, do not accumulate listener registrations on the finished-job IMaps.
     */
    @Test
    public void testCloseRemovesFinishedJobEntryListeners() {
        IMap<Long, JobHistoryService.JobState> finishedJobStateImap =
                instance.getMap(Constant.IMAP_FINISHED_JOB_STATE);
        IMap<Long, JobMetrics> finishedJobMetricsImap =
                instance.getMap(Constant.IMAP_FINISHED_JOB_METRICS);
        IMap<Long, JobDAGInfo> finishedJobDAGInfoImap =
                instance.getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO);

        // Positive control: registration ids of a service that is not closed are live, so
        // removeEntryListener returns true for them. This proves the ids exposed by the service
        // are real registrations and keeps the negative assertions below meaningful.
        JobHistoryService liveService = newJobHistoryService();
        List<UUID> liveIds = liveService.getEntryListenerRegistrationIds();
        Assertions.assertEquals(3, liveIds.size());
        Assertions.assertTrue(finishedJobStateImap.removeEntryListener(liveIds.get(0)));
        Assertions.assertTrue(finishedJobMetricsImap.removeEntryListener(liveIds.get(1)));
        Assertions.assertTrue(finishedJobDAGInfoImap.removeEntryListener(liveIds.get(2)));

        // Repeated create/close cycles must not leave any registration behind: after close(),
        // removing the same registration id again returns false because the listener is already
        // deregistered from the map.
        for (int i = 0; i < 3; i++) {
            JobHistoryService jobHistoryService = newJobHistoryService();
            List<UUID> registrationIds = jobHistoryService.getEntryListenerRegistrationIds();
            Assertions.assertEquals(3, registrationIds.size());

            jobHistoryService.close();

            Assertions.assertFalse(
                    finishedJobStateImap.removeEntryListener(registrationIds.get(0)));
            Assertions.assertFalse(
                    finishedJobMetricsImap.removeEntryListener(registrationIds.get(1)));
            Assertions.assertFalse(
                    finishedJobDAGInfoImap.removeEntryListener(registrationIds.get(2)));
        }
    }

    /**
     * Verifies that clearCoordinatorService() deregisters the listeners of the active
     * JobHistoryService, which is the production cleanup path executed when a node leaves the
     * active master role. Uses an isolated Hazelcast instance so that clearing the coordinator does
     * not disturb the shared test node.
     */
    @Test
    public void testClearCoordinatorServiceDeregistersJobHistoryListeners() {
        HazelcastInstanceImpl coordinatorInstance =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName(
                                "JobHistoryServiceListenerCleanupTest_clearCoordinatorService"));
        try {
            SeaTunnelServer seaTunnelServer =
                    coordinatorInstance
                            .node
                            .getNodeEngine()
                            .getService(SeaTunnelServer.SERVICE_NAME);
            CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();
            JobHistoryService jobHistoryService = coordinatorService.getJobHistoryService();
            List<UUID> registrationIds = jobHistoryService.getEntryListenerRegistrationIds();
            Assertions.assertEquals(3, registrationIds.size());

            coordinatorService.clearCoordinatorService();

            // The registration ids captured before the clear must be gone afterwards. A new
            // JobHistoryService created by a later re-activation registers new ids, so these
            // assertions stay valid even if the master-check scheduler re-initializes the
            // coordinator in the background.
            IMap<Long, JobHistoryService.JobState> finishedJobStateImap =
                    coordinatorInstance.getMap(Constant.IMAP_FINISHED_JOB_STATE);
            IMap<Long, JobMetrics> finishedJobMetricsImap =
                    coordinatorInstance.getMap(Constant.IMAP_FINISHED_JOB_METRICS);
            IMap<Long, JobDAGInfo> finishedJobDAGInfoImap =
                    coordinatorInstance.getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO);
            Assertions.assertFalse(
                    finishedJobStateImap.removeEntryListener(registrationIds.get(0)));
            Assertions.assertFalse(
                    finishedJobMetricsImap.removeEntryListener(registrationIds.get(1)));
            Assertions.assertFalse(
                    finishedJobDAGInfoImap.removeEntryListener(registrationIds.get(2)));
        } finally {
            coordinatorInstance.shutdown();
        }
    }

    /**
     * Creates a JobHistoryService against the shared test node with empty pending and running job
     * views, which is sufficient for listener registration lifecycle checks.
     */
    private JobHistoryService newJobHistoryService() {
        return new JobHistoryService(
                nodeEngine,
                instance.getMap(Constant.IMAP_RUNNING_JOB_STATE),
                nodeEngine.getLogger(JobHistoryServiceListenerCleanupTest.class),
                new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(),
                instance.getMap(Constant.IMAP_FINISHED_JOB_STATE),
                instance.getMap(Constant.IMAP_FINISHED_JOB_METRICS),
                instance.getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO),
                1);
    }
}
