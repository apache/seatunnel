/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.maintest;

import org.apache.seatunnel.engine.api.context.LocalExecutionContext;
import org.apache.seatunnel.engine.api.example.SimpleSink;
import org.apache.seatunnel.engine.api.example.SimpleSource;
import org.apache.seatunnel.engine.config.Configuration;
import org.apache.seatunnel.engine.execution.TaskExecution;
import org.apache.seatunnel.engine.jobmanager.JobMaster;
import org.apache.seatunnel.engine.jobmanager.JobMasterId;
import org.apache.seatunnel.engine.logicalplan.LogicalPlan;
import org.apache.seatunnel.engine.utils.SeaTunnelExecutionThreadFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class SeaTunnelEngineMainTest {
    protected final Logger logger = LoggerFactory.getLogger(SeaTunnelEngineMainTest.class);

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void testBoundedJob() throws Exception {
        //shared executor service
        final ScheduledExecutorService futureExecutor =
            Executors.newScheduledThreadPool(
                10, new SeaTunnelExecutionThreadFactory("st-engine-execution-"));

        //Create TaskExecution
        //It will have only one TaskExecution instance in a standalone mode(All Server run in a same JVM)
        //TaskExecution will deploy one or more nodes in standalone cluster mode
        TaskExecution taskExecution = new TaskExecution(futureExecutor, null);

        // ------ this code will run in engine client after we add the client and server module ---------//
        // ----------------------------------------------------------------------------------------------//
        // ----------------------------------------------------------------------------------------------//
        LocalExecutionContext localExecutionContext = new LocalExecutionContext(new Configuration());
        localExecutionContext.setSource(new SimpleSource());
        localExecutionContext.setTransformations(null);
        localExecutionContext.setSink(new SimpleSink());
        localExecutionContext.setJobName("Test Job");
        localExecutionContext.setMaxParallelism(2);

        LogicalPlan logicalPlan = localExecutionContext.getLogicalPlan();

        // ------ this code will run in jobmanager after we add the jobmanager server module ------------//
        // ----------------------------------------------------------------------------------------------//
        // ----------------------------------------------------------------------------------------------//

        JobMaster jobMaster = new JobMaster(JobMasterId.generate(),
            localExecutionContext.getConfiguration(),
            logicalPlan,
            futureExecutor,
            System.currentTimeMillis());

        // All jobMaster can only use the same taskExecution now
        jobMaster.registerTaskExecution(taskExecution);

        // TaskExecution can run many job
        taskExecution.registerJobMaster(logicalPlan.getJobInformation().getJobId(), jobMaster);
        jobMaster.run();
    }
}
