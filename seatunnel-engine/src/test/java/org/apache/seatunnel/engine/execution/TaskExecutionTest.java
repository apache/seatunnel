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

package org.apache.seatunnel.engine.execution;

import org.apache.seatunnel.engine.api.common.JobID;
import org.apache.seatunnel.engine.api.example.SimpleSinkWriter;
import org.apache.seatunnel.engine.api.example.SimpleSourceReader;
import org.apache.seatunnel.engine.api.source.Boundedness;
import org.apache.seatunnel.engine.config.Configuration;
import org.apache.seatunnel.engine.executionplan.ExecutionId;
import org.apache.seatunnel.engine.executionplan.JobInformation;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.Executors;

public class TaskExecutionTest {

    @SuppressWarnings("checkstyle:MagicNumber")
    @Test
    public void testRunTask() throws InterruptedException {
        JobInformation jobInformation = new JobInformation(new JobID(), "test fake job", new Configuration(), Boundedness.BOUNDED);

        TaskInfo taskInfo = new TaskInfo(
            jobInformation,
            new ExecutionId(),
            1,
            Boundedness.BOUNDED,
            new SimpleSourceReader(),
            new ArrayList<>(),
            new SimpleSinkWriter());

        TaskExecution taskExecution = new TaskExecution(Executors.newFixedThreadPool(2), null);

        taskExecution.submit(taskInfo);

        Thread.sleep(500L);

        Assert.assertEquals(taskExecution.aliveTaskSize(), 1);

        Thread.sleep(5000L);

        Assert.assertEquals(taskExecution.aliveTaskSize(), 0);

    }
}
