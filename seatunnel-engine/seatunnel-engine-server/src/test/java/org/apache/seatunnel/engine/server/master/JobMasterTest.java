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

import static org.awaitility.Awaitility.await;

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.TestUtils;

import com.hazelcast.internal.serialization.Data;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * JobMaster Tester.
 */
public class JobMasterTest extends AbstractSeaTunnelServerTest {
    private Long jobId;

    @Before
    public void before() {
        super.before();
        jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
    }

    @Test
    public void testHandleCheckpointTimeout() throws Exception {
        SeaTunnelContext.getContext().setJobMode(JobMode.STREAMING);
        LogicalDag testLogicalDag = TestUtils.getTestLogicalDag();
        JobConfig config = new JobConfig();
        config.setName("test_checkpoint_timeout");

        JobImmutableInformation jobImmutableInformation = new JobImmutableInformation(jobId,
            nodeEngine.getSerializationService().toData(testLogicalDag), config, Collections.emptyList());

        Data data = nodeEngine.getSerializationService().toData(jobImmutableInformation);

        PassiveCompletableFuture<Void> voidPassiveCompletableFuture = server.submitJob(jobId, data);
        voidPassiveCompletableFuture.join();

        JobMaster jobMaster = server.getJobMaster(jobId);

        // waiting for job status turn to running
        await().atMost(10000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assert.assertEquals(JobStatus.RUNNING, jobMaster.getJobStatus()));

        // call checkpoint timeout
        jobMaster.handleCheckpointTimeout(1);
        // test job still run
        await().atMost(20000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assert.assertEquals(JobStatus.RUNNING, jobMaster.getJobStatus()));

        PassiveCompletableFuture<JobStatus> jobMasterCompleteFuture = jobMaster.getJobMasterCompleteFuture();
        // cancel job
        jobMaster.cancelJob();

        // test job turn to complete
        await().atMost(20000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assert.assertTrue(
                jobMasterCompleteFuture.isDone() && JobStatus.CANCELED.equals(jobMasterCompleteFuture.get())));
    }
}
