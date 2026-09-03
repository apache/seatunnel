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

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.serializable.JobDataSerializerHook;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JobInfoServiceNullSafetyTest {

    private final Long jobId = 1L;

    private JobInfoService jobInfoService;
    private NodeEngineImpl nodeEngine;
    private HazelcastInstance hazelcastInstance;
    private InternalSerializationService serializationService;

    private IMap<Object, Object> runningJobInfoMap;
    private IMap<Object, Object> finishedJobStateMap;
    private IMap<Object, Object> finishedJobMetricsMap;
    private IMap<Object, Object> finishedJobVertexInfoMap;

    @BeforeEach
    void setUp() {
        nodeEngine = mock(NodeEngineImpl.class);
        hazelcastInstance = mock(HazelcastInstance.class);
        serializationService =
                (InternalSerializationService) new DefaultSerializationServiceBuilder().build();

        runningJobInfoMap = mock(IMap.class);
        finishedJobStateMap = mock(IMap.class);
        finishedJobMetricsMap = mock(IMap.class);
        finishedJobVertexInfoMap = mock(IMap.class);

        when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        when(nodeEngine.getSerializationService()).thenReturn(serializationService);
        when(hazelcastInstance.getMap(Constant.IMAP_RUNNING_JOB_INFO))
                .thenReturn(runningJobInfoMap);
        when(hazelcastInstance.getMap(Constant.IMAP_FINISHED_JOB_STATE))
                .thenReturn(finishedJobStateMap);
        when(hazelcastInstance.getMap(Constant.IMAP_FINISHED_JOB_METRICS))
                .thenReturn(finishedJobMetricsMap);
        when(hazelcastInstance.getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO))
                .thenReturn(finishedJobVertexInfoMap);

        jobInfoService = new JobInfoService(nodeEngine);
    }

    @Test
    void shouldDecodeLegacyJobImmutableInformationInFastPath() throws Exception {
        long createTime = 123456L;
        JobInfo jobInfo =
                new JobInfo(
                        1L,
                        serializationService.toData(
                                new LegacyJobImmutableInformation(jobId, createTime)));

        Object basicInfo = invokeDecodeJobBasicInfo(jobInfo);

        Assertions.assertNotNull(basicInfo);
        Assertions.assertEquals("legacy-job", getFieldValue(basicInfo, "jobName"));
        Assertions.assertEquals(createTime, getFieldValue(basicInfo, "createTime"));
    }

    private JobHistoryService.JobState buildJobState(Long jobId, Long startTime, Long finishTime) {
        return new JobHistoryService.JobState(
                jobId,
                "test-job",
                JobStatus.FAILED,
                System.currentTimeMillis(),
                startTime,
                finishTime,
                Collections.emptyMap(),
                null);
    }

    @Test
    void shouldReturnJobIdOnlyWhenFinishedMetricsIsMissing() {
        JobHistoryService.JobState jobState = buildJobState(jobId, 1000L, 2000L);

        when(runningJobInfoMap.get(jobId)).thenReturn(null);
        when(finishedJobStateMap.get(jobId)).thenReturn(jobState);
        when(finishedJobMetricsMap.get(jobId)).thenReturn(null);

        JsonObject result = jobInfoService.getJobInfoJson(jobId);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(jobId.toString(), result.getString(RestConstant.JOB_ID, null));
    }

    @Test
    void shouldReturnFinishedJobInfoWhenFinishedStateAndMetricsExist() {
        JobHistoryService.JobState jobState = buildJobState(jobId, 1000L, 2000L);

        JobMetrics jobMetrics = mock(JobMetrics.class);
        when(jobMetrics.toJsonString()).thenReturn("{}");

        JobDAGInfo dagInfo = mock(JobDAGInfo.class);
        JsonObject dagJson = new JsonObject().add("key", "value");
        when(dagInfo.toJsonObject()).thenReturn(dagJson);

        when(runningJobInfoMap.get(jobId)).thenReturn(null);
        when(finishedJobStateMap.get(jobId)).thenReturn(jobState);
        when(finishedJobMetricsMap.get(jobId)).thenReturn(jobMetrics);
        when(finishedJobVertexInfoMap.get(jobId)).thenReturn(dagInfo);

        JsonObject result = jobInfoService.getJobInfoJson(jobId);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(jobId.toString(), result.getString(RestConstant.JOB_ID, null));
        Assertions.assertEquals(dagJson.toString(), result.get(RestConstant.JOB_DAG).toString());
    }

    @Test
    void shouldReturnJobIdOnlyWhenFinishedStateDoesNotExist() {
        when(runningJobInfoMap.get(jobId)).thenReturn(null);
        when(finishedJobStateMap.get(jobId)).thenReturn(null);
        when(finishedJobMetricsMap.get(jobId)).thenReturn(null);

        JsonObject result = jobInfoService.getJobInfoJson(jobId);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(jobId.toString(), result.getString(RestConstant.JOB_ID, null));
    }

    @Test
    void shouldListFinishedJobsWhenFinishTimeMetricsOrDagAreMissing() {
        Long jobWithFinishTime = 2L;
        JobHistoryService.JobState missingFinishTime = buildJobState(jobId, 1000L, null);
        JobHistoryService.JobState completed = buildJobState(jobWithFinishTime, 1000L, 2000L);

        when(finishedJobStateMap.values())
                .thenReturn((java.util.Collection) Arrays.asList(missingFinishTime, completed));
        when(finishedJobMetricsMap.getOrDefault(jobId, JobMetrics.empty())).thenReturn(null);
        when(finishedJobMetricsMap.getOrDefault(jobWithFinishTime, JobMetrics.empty()))
                .thenThrow(new RuntimeException("metrics unavailable"));
        when(finishedJobVertexInfoMap.get(jobId))
                .thenThrow(new RuntimeException("dag unavailable"));
        when(finishedJobVertexInfoMap.get(jobWithFinishTime)).thenReturn(null);

        JsonArray result = jobInfoService.getJobsByStateJson("");

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(
                jobWithFinishTime.toString(),
                result.get(0).asObject().getString(RestConstant.JOB_ID, null));
        Assertions.assertEquals(
                jobId.toString(), result.get(1).asObject().getString(RestConstant.JOB_ID, null));
    }

    private Object invokeDecodeJobBasicInfo(JobInfo jobInfo) throws Exception {
        Method method = JobInfoService.class.getDeclaredMethod("decodeJobBasicInfo", JobInfo.class);
        method.setAccessible(true);
        return method.invoke(jobInfoService, jobInfo);
    }

    private Object getFieldValue(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static final class LegacyJobImmutableInformation
            implements com.hazelcast.nio.serialization.IdentifiedDataSerializable {

        private final long jobId;
        private final long createTime;

        private LegacyJobImmutableInformation(long jobId, long createTime) {
            this.jobId = jobId;
            this.createTime = createTime;
        }

        @Override
        public int getFactoryId() {
            return JobDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JobDataSerializerHook.JOB_IMMUTABLE_INFORMATION;
        }

        @Override
        public void writeData(com.hazelcast.nio.ObjectDataOutput out) throws IOException {
            out.writeLong(jobId);
            out.writeString("legacy-job");
            out.writeBoolean(true);
            out.writeLong(createTime);
            out.writeInt(0);
            IOUtil.writeData(out, null);
            out.writeObject(null);
            out.writeObject(null);
            out.writeObject(null);
        }

        @Override
        public void readData(com.hazelcast.nio.ObjectDataInput in) {
            throw new UnsupportedOperationException("write-only test fixture");
        }
    }
}
