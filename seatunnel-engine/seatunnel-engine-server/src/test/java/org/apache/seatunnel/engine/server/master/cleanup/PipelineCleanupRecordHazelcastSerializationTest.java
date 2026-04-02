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

package org.apache.seatunnel.engine.server.master.cleanup;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

class PipelineCleanupRecordHazelcastSerializationTest {

    @Test
    void testPutAndGetAcrossMembers() {
        String clusterName =
                TestUtils.getClusterName(
                        "PipelineCleanupRecordHazelcastSerializationTest_testPutAndGetAcrossMembers");
        HazelcastInstanceImpl instance1 =
                SeaTunnelServerStarter.createHazelcastInstance(clusterName);
        HazelcastInstanceImpl instance2 =
                SeaTunnelServerStarter.createHazelcastInstance(clusterName);
        try {
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> instance1.getCluster().getMembers().size() == 2);

            PipelineLocation pipelineLocation = new PipelineLocation(1L, 1);
            TaskGroupLocation taskGroupLocation = new TaskGroupLocation(1L, 1, 1L);
            Address workerAddress = instance1.getCluster().getLocalMember().getAddress();
            Map<TaskGroupLocation, Address> taskGroups = new HashMap<>();
            taskGroups.put(taskGroupLocation, workerAddress);

            PipelineCleanupRecord record =
                    new PipelineCleanupRecord(
                            pipelineLocation,
                            PipelineStatus.CANCELED,
                            false,
                            taskGroups,
                            new HashSet<>(Collections.singleton(taskGroupLocation)),
                            true,
                            100L,
                            200L,
                            3);

            IMap<PipelineLocation, PipelineCleanupRecord> map1 =
                    instance1.getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
            IMap<PipelineLocation, PipelineCleanupRecord> map2 =
                    instance2.getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);

            map1.put(pipelineLocation, record);

            await().atMost(30, TimeUnit.SECONDS).until(() -> map2.containsKey(pipelineLocation));

            PipelineCleanupRecord read = map2.get(pipelineLocation);
            Assertions.assertNotNull(read);
            Assertions.assertEquals(pipelineLocation, read.getPipelineLocation());
            Assertions.assertEquals(PipelineStatus.CANCELED, read.getFinalStatus());
            Assertions.assertFalse(read.isSavepointEnd());
            Assertions.assertTrue(read.isMetricsImapCleaned());
            Assertions.assertEquals(100L, read.getCreateTimeMillis());
            Assertions.assertEquals(200L, read.getLastAttemptTimeMillis());
            Assertions.assertEquals(3, read.getAttemptCount());
            Assertions.assertEquals(workerAddress, read.getTaskGroups().get(taskGroupLocation));
            Assertions.assertTrue(read.getCleanedTaskGroups().contains(taskGroupLocation));
            Assertions.assertTrue(read.isCleaned());
        } finally {
            instance1.shutdown();
            instance2.shutdown();
        }
    }
}
