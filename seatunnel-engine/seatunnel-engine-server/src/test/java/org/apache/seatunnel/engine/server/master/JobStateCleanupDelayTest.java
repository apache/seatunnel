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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.rest.service.JobInfoService;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.concurrent.TimeUnit;

class JobStateCleanupDelayTest extends AbstractSeaTunnelServerTest<JobStateCleanupDelayTest> {

    private static final long STATE_CLEANUP_DELAY_MILLIS = 200L;

    @Override
    public SeaTunnelConfig loadSeaTunnelConfig() {
        SeaTunnelConfig seaTunnelConfig = super.loadSeaTunnelConfig();
        seaTunnelConfig.getEngineConfig().setStateCleanupDelayMillis(STATE_CLEANUP_DELAY_MILLIS);
        return seaTunnelConfig;
    }

    @Test
    void testTerminalStateRetainedBeforeDelayedCleanup() {
        long jobId = System.currentTimeMillis();
        startJob(jobId, "batch_fake_to_console_without_checkpoint_interval.conf", false);

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FINISHED,
                                        server.getCoordinatorService().getJobStatus(jobId)));

        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);

        Assertions.assertNotNull(runningJobInfoIMap.get(jobId));
        Assertions.assertEquals(JobStatus.FINISHED, runningJobStateIMap.get(jobId));
        Assertions.assertNotNull(runningJobStateTimestampsIMap.get(jobId));
        Assertions.assertTrue(
                new JobInfoService((NodeEngineImpl) nodeEngine).getRunningJobsJson().isEmpty());

        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertNull(runningJobInfoIMap.get(jobId));
                            Assertions.assertNull(runningJobStateIMap.get(jobId));
                            Assertions.assertNull(runningJobStateTimestampsIMap.get(jobId));
                        });
    }
}
