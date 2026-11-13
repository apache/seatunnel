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
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.serializable.JobDataSerializerHook;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

        Object basicInfo = invokeDecodeJobBasicInfo(jobId, jobInfo);

        Assertions.assertNotNull(basicInfo);
        Assertions.assertEquals("legacy-job", getFieldValue(basicInfo, "jobName"));
        Assertions.assertEquals(createTime, getFieldValue(basicInfo, "createTime"));
    }

    /**
     * The fast path must read the bytes that {@code SerializationService.toData} really produces
     * and return what the normal deserialization returns. It is invoked directly, so the full
     * deserialization fallback cannot make this pass.
     */
    @Test
    void shouldDecodeSameBasicInfoInFastPathAsFullDeserialization() throws Exception {
        long[] jobIds = {1L, 0x0123456789ABCD01L, 1088526349862699008L, -1L};
        String[] jobNames = {"cdc_example", "", "任务-ジョブ-job", null};

        for (long id : jobIds) {
            for (String jobName : jobNames) {
                Data data = serializationService.toData(buildJobImmutableInformation(id, jobName));
                JobImmutableInformation expected = serializationService.toObject(data);

                Object basicInfo = invokeDecodeJobBasicInfoFromPrefix(id, data);

                Assertions.assertNotNull(basicInfo, "fast path rejected job " + id);
                Assertions.assertEquals(expected.getJobName(), getFieldValue(basicInfo, "jobName"));
                Assertions.assertEquals(
                        expected.getCreateTime(), getFieldValue(basicInfo, "createTime"));
            }
        }
    }

    /**
     * Regression for a fast path that started reading one byte too early: the job name length was
     * then taken from the last byte of the job id and the first three bytes of the real length. For
     * a job id ending in 0x01 that is 16,777,216, and once the serialized information was larger
     * than that the read succeeded and returned 16 MB of the payload as the job name.
     */
    @Test
    void shouldReturnBoundedSummaryForLargeJobImmutableInformation() {
        long largeJobId = 0x0F1B2C3D4E5F6A01L;
        String jobName = "cdc_example";
        JobImmutableInformation information =
                buildJobImmutableInformation(largeJobId, jobName, 20 * 1024 * 1024);
        Data data = serializationService.toData(information);
        Assertions.assertTrue(data.totalSize() > (1 << 24) + 1024);

        mockRunningJob(largeJobId, new JobInfo(System.currentTimeMillis(), data));

        JsonArray summary = jobInfoService.getRunningJobsSummaryJson();

        Assertions.assertEquals(1, summary.size());
        // Checked first: a failure here reports a number instead of megabytes of job name.
        int summaryLength = summary.toString().length();
        Assertions.assertTrue(summaryLength < 512, "summary length was " + summaryLength);
        JsonObject job = summary.get(0).asObject();
        Assertions.assertEquals(
                String.valueOf(largeJobId), job.getString(RestConstant.JOB_ID, null));
        Assertions.assertEquals(jobName, job.getString(RestConstant.JOB_NAME, null));
        Assertions.assertEquals(
                DateTimeUtils.toString(
                        information.getCreateTime(), DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                job.getString(RestConstant.CREATE_TIME, null));
    }

    /** Data the fast path does not trust still gets decoded, by the full deserialization. */
    @Test
    void shouldFallBackToFullDeserializationWhenFastPathRejectsData() throws Exception {
        char[] longNameChars = new char[100_000];
        Arrays.fill(longNameChars, 'n');
        String longName = new String(longNameChars);
        JobImmutableInformation longNamed = buildJobImmutableInformation(jobId, longName);
        Data longNamedData = serializationService.toData(longNamed);

        Assertions.assertNull(invokeDecodeJobBasicInfoFromPrefix(jobId, longNamedData));
        Object basicInfo = invokeDecodeJobBasicInfo(jobId, new JobInfo(1L, longNamedData));
        Assertions.assertEquals(longName, getFieldValue(basicInfo, "jobName"));
        Assertions.assertEquals(longNamed.getCreateTime(), getFieldValue(basicInfo, "createTime"));

        // A job id that differs from the map key means the layout is not the expected one.
        JobImmutableInformation other = buildJobImmutableInformation(2L, "other-job");
        Data otherData = serializationService.toData(other);

        Assertions.assertNull(invokeDecodeJobBasicInfoFromPrefix(jobId, otherData));
        basicInfo = invokeDecodeJobBasicInfo(jobId, new JobInfo(1L, otherData));
        Assertions.assertEquals("other-job", getFieldValue(basicInfo, "jobName"));
        Assertions.assertEquals(other.getCreateTime(), getFieldValue(basicInfo, "createTime"));

        // Not a DataSerializable at all.
        Assertions.assertNull(
                invokeDecodeJobBasicInfoFromPrefix(jobId, serializationService.toData("job")));
    }

    /**
     * A paged request must pay the per-job metrics and DAG lookups for the rows it returns, not for
     * every retained job. Verifying the lookup count is the point of the test: asserting only on
     * the returned JSON would still pass if the whole listing were built and then sliced.
     */
    @Test
    void shouldApplyPageBeforePerJobLookups() {
        List<Object> storedStates = new ArrayList<>();
        for (int index = 0; index < 25; index++) {
            storedStates.add(buildJobState((long) index, 1000L, 2000L + index));
        }
        when(finishedJobStateMap.values()).thenReturn(storedStates);
        when(finishedJobMetricsMap.getOrDefault(any(), any())).thenReturn(JobMetrics.empty());
        when(finishedJobVertexInfoMap.get(any())).thenReturn(null);

        JobInfoService.JobPage page = jobInfoService.getJobsByStateJson("", 0, 10);

        Assertions.assertEquals(10, page.getData().size());
        Assertions.assertEquals(25, page.getTotal(), "total must count matches before slicing");
        verify(finishedJobMetricsMap, times(10)).getOrDefault(any(), any());
        verify(finishedJobVertexInfoMap, times(10)).get(any());
    }

    /** Newest finish time first, so that paging over a growing history stays stable. */
    @Test
    void shouldReturnNewestFinishedJobsFirst() {
        List<Object> storedStates = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            storedStates.add(buildJobState((long) index, 1000L, 2000L + index));
        }
        when(finishedJobStateMap.values()).thenReturn(storedStates);
        when(finishedJobMetricsMap.getOrDefault(any(), any())).thenReturn(JobMetrics.empty());
        when(finishedJobVertexInfoMap.get(any())).thenReturn(null);

        JobInfoService.JobPage page = jobInfoService.getJobsByStateJson("", 0, 2);

        Assertions.assertEquals(2, page.getData().size());
        Assertions.assertEquals(
                "4", page.getData().get(0).asObject().getString(RestConstant.JOB_ID, null));
        Assertions.assertEquals(
                "3", page.getData().get(1).asObject().getString(RestConstant.JOB_ID, null));
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

    private JobImmutableInformation buildJobImmutableInformation(long id, String jobName) {
        return buildJobImmutableInformation(id, jobName, 0);
    }

    /** {@code paddingBytes} inflates the serialized form the way a job with many tables does. */
    private JobImmutableInformation buildJobImmutableInformation(
            long id, String jobName, int paddingBytes) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        if (paddingBytes > 0) {
            jobConfig.getEnvOptions().put("padding", new byte[paddingBytes]);
        }
        return new JobImmutableInformation(
                id,
                jobName,
                serializationService,
                new LogicalDag(jobConfig, new IdGenerator()),
                Collections.emptyList(),
                Collections.emptyList());
    }

    private void mockRunningJob(long id, JobInfo jobInfo) {
        Node node = mock(Node.class);
        org.apache.seatunnel.engine.server.NodeExtension nodeExtension =
                mock(org.apache.seatunnel.engine.server.NodeExtension.class);
        SeaTunnelServer seaTunnelServer = mock(SeaTunnelServer.class);
        CoordinatorService coordinatorService = mock(CoordinatorService.class);
        when(nodeEngine.getNode()).thenReturn(node);
        when(node.getNodeExtension()).thenReturn(nodeExtension);
        when(nodeExtension.getSeaTunnelServer()).thenReturn(seaTunnelServer);
        when(seaTunnelServer.isMasterNode()).thenReturn(true);
        when(seaTunnelServer.getCoordinatorService()).thenReturn(coordinatorService);
        when(coordinatorService.shouldShowAsRunningJob(id)).thenReturn(true);

        Map.Entry<Object, Object> entry = new AbstractMap.SimpleEntry<>(id, jobInfo);
        when(runningJobInfoMap.entrySet()).thenReturn(Collections.singleton(entry));
    }

    private Object invokeDecodeJobBasicInfo(long id, JobInfo jobInfo) throws Exception {
        Method method =
                JobInfoService.class.getDeclaredMethod(
                        "decodeJobBasicInfo", long.class, JobInfo.class);
        method.setAccessible(true);
        return method.invoke(jobInfoService, id, jobInfo);
    }

    private Object invokeDecodeJobBasicInfoFromPrefix(long id, Data data) throws Exception {
        Method method =
                JobInfoService.class.getDeclaredMethod(
                        "decodeJobBasicInfoFromPrefix", long.class, Data.class);
        method.setAccessible(true);
        return method.invoke(jobInfoService, id, data);
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
