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

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.service.JobInfoService;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.concurrent.TimeUnit;

class FollowerRunningJobsFilterTest
        extends AbstractSeaTunnelServerTest<FollowerRunningJobsFilterTest> {

    private static final long STATE_CLEANUP_DELAY_MILLIS = 200L;

    @Override
    public SeaTunnelConfig loadSeaTunnelConfig() {
        SeaTunnelConfig seaTunnelConfig = super.loadSeaTunnelConfig();
        seaTunnelConfig.getEngineConfig().setStateCleanupDelayMillis(STATE_CLEANUP_DELAY_MILLIS);
        return seaTunnelConfig;
    }

    @Test
    void testFollowerRunningJobsApiHidesRetainedTerminalJob() {
        HazelcastInstanceImpl follower = null;
        try {
            SeaTunnelConfig seaTunnelConfig = loadSeaTunnelConfig();
            seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);

            Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
            hazelcastConfig.setClusterName(instance.getConfig().getClusterName());
            seaTunnelConfig.setHazelcastConfig(hazelcastConfig);

            follower = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, instance.getCluster().getMembers().size()));

            long jobId = System.currentTimeMillis();
            startJob(jobId, "batch_fake_to_console_without_checkpoint_interval.conf", false);

            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.FINISHED,
                                            server.getCoordinatorService().getJobStatus(jobId)));

            JobInfoService followerJobInfoService =
                    new JobInfoService((NodeEngineImpl) follower.node.nodeEngine);
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            followerJobInfoService.getRunningJobsJson().isEmpty()));
        } finally {
            if (follower != null) {
                follower.shutdown();
            }
        }
    }
}
