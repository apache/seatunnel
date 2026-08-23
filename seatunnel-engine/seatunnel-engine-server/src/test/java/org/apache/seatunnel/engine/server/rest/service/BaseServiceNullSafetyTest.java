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

import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.job.DirtyJobEvaluationStatus;
import org.apache.seatunnel.engine.common.job.DirtyJobState;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.job.MemberLeaveClassification;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    @Test
    public void testGetJobInfoJsonWithDirtyJobState() {
        JobHistoryService.JobState jobState = buildJobState(1000L, 2000L);
        DirtyJobState dirtyJobState = DirtyJobState.create(12345L, 1, 10, 600_000L, 0L);
        dirtyJobState.processMemberEvent(
                1L,
                "member-1",
                "10.0.0.1:5801",
                MemberLeaveClassification.UNCLASSIFIED,
                Collections.singletonList(1),
                100L);
        String attemptId = dirtyJobState.prepareRecoveryAttempt(1, 110L);
        dirtyJobState.confirmRecoveryAttempt(1, attemptId, 120L);
        jobState.setDirtyTask(dirtyJobState);

        JsonObject result = jobInfoService.getJobInfoJson(jobState, "{}", null);
        JsonObject dirtyTask = result.get(RestConstant.DIRTY_TASK).asObject();

        Assertions.assertTrue(dirtyTask.getBoolean("dirty", false));
        Assertions.assertEquals("DIRTY", dirtyTask.getString("evaluationStatus", null));
        Assertions.assertEquals(1, dirtyTask.getInt("recoveryAttemptCount", 0));
        Assertions.assertEquals(
                "UNCLASSIFIED", dirtyTask.getString("lastLeaveClassification", null));
    }

    @Test
    public void testMissingEnabledDirtyJobStateIsUnknown() {
        DirtyJobState dirtyJobState = jobInfoService.createUnknownDirtyJobState(12345L, 2);
        JsonObject jobSummary = new JsonObject();

        jobInfoService.addDirtyJobSummary(jobSummary, dirtyJobState);

        JsonObject dirtyTask = jobSummary.get(RestConstant.DIRTY_TASK).asObject();
        Assertions.assertFalse(dirtyTask.getBoolean("dirty", true));
        Assertions.assertEquals("UNKNOWN", dirtyTask.getString("evaluationStatus", null));
        Assertions.assertEquals(0, dirtyTask.getInt("recoveryAttemptCount", -1));
    }

    @Test
    public void testResolveConfiguredDirtyJobStateAcceptsCurrentExecution() {
        JobImmutableInformation jobInformation = createDirtyJobInformation(12345L, 200L, 2);
        DirtyJobState persistedState = DirtyJobState.create(12345L, 200L, 2, 10, 600_000L, 0L);

        DirtyJobState resolvedState =
                jobInfoService.resolveConfiguredDirtyJobState(jobInformation, persistedState);

        Assertions.assertSame(persistedState, resolvedState);
        Assertions.assertEquals(
                DirtyJobEvaluationStatus.CLEAN, resolvedState.getEvaluationStatus());
    }

    @Test
    public void testResolveConfiguredDirtyJobStateRejectsPreviousExecution() {
        JobImmutableInformation jobInformation = createDirtyJobInformation(12345L, 200L, 2);
        DirtyJobState persistedState = DirtyJobState.create(12345L, 100L, 2, 10, 600_000L, 0L);
        persistedState.processMemberEvent(
                1L,
                "member-1",
                "10.0.0.1:5801",
                MemberLeaveClassification.UNCLASSIFIED,
                Collections.singletonList(1),
                100L);
        String attemptId = persistedState.prepareRecoveryAttempt(1, 110L);
        persistedState.confirmRecoveryAttempt(1, attemptId, 120L);

        DirtyJobState resolvedState =
                jobInfoService.resolveConfiguredDirtyJobState(jobInformation, persistedState);

        Assertions.assertNotSame(persistedState, resolvedState);
        Assertions.assertEquals(200L, resolvedState.getJobCreateTime());
        Assertions.assertEquals(
                DirtyJobEvaluationStatus.UNKNOWN, resolvedState.getEvaluationStatus());
        Assertions.assertEquals(0, resolvedState.getRecoveryAttemptCount());
    }

    @Test
    public void testMetricsToJsonObjectWithNonFiniteFloats() {
        // 1. We create an anonymous subclass of BaseService if it's abstract,
        // or just instantiate it if it's concrete.
        // Passing null for dependencies since metricsToJsonObject doesn't rely on them.
        BaseService baseService = new BaseService(null) {
                    // Dummy anonymous class in case BaseService is abstract in the codebase
                };

        // 2. Prepare the boundary condition metrics
        Map<String, Object> jobMetrics = new HashMap<>();
        jobMetrics.put("normal_double", 123.456d);
        jobMetrics.put("normal_float", 78.9f);
        jobMetrics.put("nan_double", Double.NaN);
        jobMetrics.put("nan_float", Float.NaN);
        jobMetrics.put("positive_infinity", Double.POSITIVE_INFINITY);
        jobMetrics.put("negative_infinity", Double.NEGATIVE_INFINITY);

        // 3. Nested map to test the recursive loop in your fix
        Map<String, Object> nestedMetrics = new HashMap<>();
        nestedMetrics.put("nested_nan", Double.NaN);
        jobMetrics.put("nested_map", nestedMetrics);

        // 4. Execute your fixed method
        JsonObject result = baseService.metricsToJsonObject(jobMetrics);

        // 5. Assert the values were parsed to strings safely without throwing an exception
        Assertions.assertEquals("123.456", result.getString("normal_double", ""));
        Assertions.assertEquals("78.9", result.getString("normal_float", ""));
        Assertions.assertEquals("NaN", result.getString("nan_double", ""));
        Assertions.assertEquals("NaN", result.getString("nan_float", ""));
        Assertions.assertEquals("Infinity", result.getString("positive_infinity", ""));
        Assertions.assertEquals("-Infinity", result.getString("negative_infinity", ""));

        // Check the nested map
        JsonObject nestedResult = result.get("nested_map").asObject();
        Assertions.assertEquals("NaN", nestedResult.get("nested_nan").asString());
    }

    private JobImmutableInformation createDirtyJobInformation(
            long jobId, long createTime, int threshold) {
        JobImmutableInformation jobInformation = mock(JobImmutableInformation.class);
        JobConfig jobConfig = mock(JobConfig.class);
        when(jobInformation.getJobId()).thenReturn(jobId);
        when(jobInformation.getCreateTime()).thenReturn(createTime);
        when(jobInformation.getJobConfig()).thenReturn(jobConfig);
        when(jobConfig.getEnvOptions())
                .thenReturn(
                        Collections.singletonMap(
                                EnvCommonOptions.DIRTY_JOB_RESTORE_THRESHOLD.key(), threshold));
        return jobInformation;
    }
}
