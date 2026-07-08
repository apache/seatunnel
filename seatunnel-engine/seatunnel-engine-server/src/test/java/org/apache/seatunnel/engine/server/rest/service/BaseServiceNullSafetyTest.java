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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collections;

import static org.mockito.Mockito.mock;

public class BaseServiceNullSafetyTest {

    private JobInfoService jobInfoService;

    @BeforeEach
    void setUp() {
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        jobInfoService = new JobInfoService(nodeEngine);
    }

    private JobHistoryService.JobState buildJobState(Long startTime, Long finishTime) {
        return new JobHistoryService.JobState(
                12345L,
                "test-job",
                JobStatus.FAILED,
                System.currentTimeMillis(),
                startTime,
                finishTime,
                Collections.emptyMap(),
                null);
    }

    @Test
    public void testGetJobInfoJsonWithNullDAGInfo() {
        JobHistoryService.JobState jobState = buildJobState(1000L, 2000L);

        JsonObject result = jobInfoService.getJobInfoJson(jobState, "{}", null);

        Assertions.assertNotNull(result);
        Assertions.assertNotNull(result.get(RestConstant.JOB_DAG));
        Assertions.assertEquals("{}", result.get(RestConstant.JOB_DAG).toString());
    }

    @Test
    public void testGetJobInfoJsonWithNonNullDAGInfo() {
        JobHistoryService.JobState jobState = buildJobState(1000L, 2000L);
        JobDAGInfo dagInfo = mock(JobDAGInfo.class);
        com.hazelcast.internal.json.JsonObject dagJson = new JsonObject().add("key", "value");
        org.mockito.Mockito.when(dagInfo.toJsonObject()).thenReturn(dagJson);

        JsonObject result = jobInfoService.getJobInfoJson(jobState, "{}", dagInfo);

        Assertions.assertEquals(dagJson.toString(), result.get(RestConstant.JOB_DAG).toString());
    }

    @Test
    public void testGetJobInfoJsonWithNullStartTime() {
        JobHistoryService.JobState jobState = buildJobState(null, 2000L);

        JsonObject result = jobInfoService.getJobInfoJson(jobState, "{}", null);

        Assertions.assertNotNull(result);
        Assertions.assertEquals("", result.getString(RestConstant.START_TIME, null));
    }

    @Test
    public void testGetJobInfoJsonWithNullFinishTime() {
        JobHistoryService.JobState jobState = buildJobState(1000L, null);

        JsonObject result = jobInfoService.getJobInfoJson(jobState, "{}", null);

        Assertions.assertNotNull(result);
        Assertions.assertEquals("", result.getString(RestConstant.FINISH_TIME, null));
    }

    @Test
    public void testGetJobInfoJsonWithBothTimestampsNull() {
        JobHistoryService.JobState jobState = buildJobState(null, null);

        JsonObject result = jobInfoService.getJobInfoJson(jobState, "{}", null);

        Assertions.assertNotNull(result);
        Assertions.assertEquals("", result.getString(RestConstant.START_TIME, null));
        Assertions.assertEquals("", result.getString(RestConstant.FINISH_TIME, null));
    }
}
