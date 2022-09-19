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

import static org.awaitility.Awaitility.await;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobStatus;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class CoordinatorServiceTest {
    @Test
    public void testMasterNodeActive() {
        HazelcastInstanceImpl instance1 = TestUtils.createHazelcastInstance("CoordinatorServiceTest_testMasterNodeActive");
        HazelcastInstanceImpl instance2 = TestUtils.createHazelcastInstance("CoordinatorServiceTest_testMasterNodeActive");

        SeaTunnelServer server1 = instance1.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        SeaTunnelServer server2 = instance2.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);

        CoordinatorService coordinatorService1 = server1.getCoordinatorService();
        Assert.assertTrue(coordinatorService1.isCoordinatorActive());

        try {
            server2.getCoordinatorService();
            Assert.fail("Need throw SeaTunnelEngineException here but not.");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SeaTunnelEngineException);
        }

        // shutdown instance1
        instance1.shutdown();
        await().atMost(10000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                try {
                    CoordinatorService coordinatorService = server2.getCoordinatorService();
                    Assert.assertTrue(coordinatorService.isMasterNode());
                } catch (SeaTunnelEngineException e) {
                    Assert.assertTrue(false);
                }
            });
        CoordinatorService coordinatorService2 = server2.getCoordinatorService();
        Assert.assertTrue(coordinatorService2.isCoordinatorActive());
        instance2.shutdown();
    }

    @SuppressWarnings("checkstyle:RegexpSingleline")
    @Test
    public void testClearCoordinatorService()
        throws MalformedURLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        HazelcastInstanceImpl coordinatorServiceTest = TestUtils.createHazelcastInstance("CoordinatorServiceTest_testClearCoordinatorService");
        SeaTunnelServer server1 = coordinatorServiceTest.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        CoordinatorService coordinatorService = server1.getCoordinatorService();
        Assert.assertTrue(coordinatorService.isCoordinatorActive());

        Long jobId = coordinatorServiceTest.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        LogicalDag testLogicalDag =
            TestUtils.createTestLogicalPlan("stream_fakesource_to_file.conf", "test_clear_coordinator_service", jobId);

        JobImmutableInformation jobImmutableInformation = new JobImmutableInformation(jobId,
            coordinatorServiceTest.getSerializationService().toData(testLogicalDag), testLogicalDag.getJobConfig(), Collections.emptyList());

        Data data = coordinatorServiceTest.getSerializationService().toData(jobImmutableInformation);

        coordinatorService.submitJob(jobId, data).join();

        // waiting for job status turn to running
        await().atMost(10000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assert.assertEquals(JobStatus.RUNNING, coordinatorService.getJobStatus(jobId)));

        Class<CoordinatorService> clazz = CoordinatorService.class;
        Method clearCoordinatorServiceMethod = clazz.getDeclaredMethod("clearCoordinatorService", null);
        clearCoordinatorServiceMethod.setAccessible(true);
        clearCoordinatorServiceMethod.invoke(coordinatorService, null);
        clearCoordinatorServiceMethod.setAccessible(false);

        // because runningJobMasterMap is empty and we have no JobHistoryServer, so return finished.
        Assert.assertTrue(JobStatus.FINISHED.equals(coordinatorService.getJobStatus(jobId)));
    }

    @Test
    public void testJobRestoreWhenMasterNodeSwitch() {
        // TODO wait CheckpointManager support restore from master node switch.
    }
}
