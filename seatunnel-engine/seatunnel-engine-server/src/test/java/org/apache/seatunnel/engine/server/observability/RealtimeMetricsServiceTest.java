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

package org.apache.seatunnel.engine.server.observability;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.CoordinatorService;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.slf4j.Log4jLogger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

public class RealtimeMetricsServiceTest {

    private final TestAppender testAppender = new TestAppender();
    private Logger serviceLogger;
    private Level originalLoggerLevel;

    @BeforeEach
    void attachTestAppender() throws Exception {
        Field serviceLoggerField = RealtimeMetricsService.class.getDeclaredField("log");
        serviceLoggerField.setAccessible(true);
        Log4jLogger slf4jLogger = (Log4jLogger) serviceLoggerField.get(null);
        Field underlyingLoggerField = Log4jLogger.class.getDeclaredField("logger");
        underlyingLoggerField.setAccessible(true);
        serviceLogger = (Logger) underlyingLoggerField.get(slf4jLogger);
        originalLoggerLevel = serviceLogger.getLevel();
        serviceLogger.setLevel(Level.WARN);
        testAppender.start();
        serviceLogger.addAppender(testAppender);
    }

    @AfterEach
    void removeTestAppender() {
        serviceLogger.removeAppender(testAppender);
        testAppender.stop();
        serviceLogger.setLevel(originalLoggerLevel);
    }

    @Test
    public void testCurrentAttemptConfigurationWinsOverHistory() throws Exception {
        TestContext context = newTestContext(Collections.singleton(1L));
        try {
            Map<String, Object> currentOptions = observabilityOptions(true, 2000L, 2);
            stubCurrentJobInfo(context, 1L, 100L, currentOptions);
            stubHistory(context.coordinatorService, 1L, observabilityOptions(false, 1000L, 1));

            ObservabilityConfig config = resolve(context.service, 1L);

            Assertions.assertTrue(config.isEnabled());
            Assertions.assertEquals(2000L, config.getBucketMs());
            Assertions.assertEquals(2, config.getRetentionMinutes());
            Mockito.verify(context.coordinatorService, Mockito.never()).getJobInfo(1L);
        } finally {
            context.service.shutdown();
        }
    }

    @Test
    public void testMissingCurrentJobInfoFallsBackToCoordinatorHistory() throws Exception {
        TestContext context = newTestContext(Collections.singleton(2L));
        try {
            Mockito.when(context.runningJobInfoMap.get(2L)).thenReturn(null);
            Map<String, Object> historyOptions = observabilityOptions(false, 4000L, 4);
            stubHistory(context.coordinatorService, 2L, historyOptions);

            ObservabilityConfig config = resolve(context.service, 2L);

            Assertions.assertFalse(config.isEnabled());
            Assertions.assertEquals(4000L, config.getBucketMs());
            Assertions.assertEquals(4, config.getRetentionMinutes());
            Mockito.verify(context.coordinatorService).getJobInfo(2L);
        } finally {
            context.service.shutdown();
        }
    }

    @Test
    public void testCorruptCurrentJobInfoWarnsOncePerAttemptAndDoesNotStopOtherJobs()
            throws Exception {
        TestContext context = newTestContext(new HashSet<>(Arrays.asList(3L, 4L)));
        try {
            Data corruptData = Mockito.mock(Data.class);
            Mockito.when(context.runningJobInfoMap.get(3L))
                    .thenReturn(
                            new JobInfo(300L, corruptData),
                            new JobInfo(300L, corruptData),
                            new JobInfo(301L, corruptData));
            Mockito.when(
                            context.serializationService.<JobImmutableInformation>toObject(
                                    corruptData))
                    .thenThrow(new IllegalArgumentException("corrupt JobInfo"));
            stubHistory(context.coordinatorService, 3L, observabilityOptions(false, 1000L, 1));
            stubCurrentJobInfo(context, 4L, 400L, observabilityOptions(true, 2000L, 2));
            Mockito.when(
                            context.coordinatorService.getRunningJobRawMetrics(
                                    Mockito.anySet(), Mockito.anyLong(), Mockito.any()))
                    .thenReturn(Collections.emptyList());

            Assertions.assertDoesNotThrow(() -> collectOnce(context.service));
            Assertions.assertDoesNotThrow(() -> collectOnce(context.service));
            Assertions.assertTrue(
                    countCorruptJobWarnings() == 1,
                    "the same corrupt (jobId, initializationTimestamp) should warn once");
            Assertions.assertDoesNotThrow(() -> collectOnce(context.service));
            Assertions.assertEquals(
                    2,
                    countCorruptJobWarnings(),
                    "a new initializationTimestamp should produce a new WARN");
            Map<String, Object> healthyJob =
                    context.service.listJobs().stream()
                            .filter(job -> Long.valueOf(4L).equals(job.get("jobId")))
                            .findFirst()
                            .orElseThrow(() -> new AssertionError("healthy job was not processed"));
            Assertions.assertEquals(true, healthyJob.get("enabled"));
            Mockito.verify(context.coordinatorService, Mockito.times(3)).getJobInfo(3L);
        } finally {
            context.service.shutdown();
        }
    }

    private long countCorruptJobWarnings() {
        return testAppender.events.stream()
                .filter(
                        event ->
                                event.getLevel() == Level.WARN
                                        && event.getMessage()
                                                .getFormattedMessage()
                                                .contains("job 3"))
                .count();
    }

    @Test
    public void testRetentionChangeInCurrentAttemptIsPickedUp() throws Exception {
        TestContext context = newTestContext(Collections.singleton(5L));
        try {
            Data firstData = Mockito.mock(Data.class);
            Data restoredData = Mockito.mock(Data.class);
            context.runningJobInfoMap.put(5L, new JobInfo(500L, firstData));
            context.runningJobInfoMap.put(5L, new JobInfo(501L, restoredData));
            stubImmutableInformation(context, firstData, observabilityOptions(true, 2000L, 1));
            stubImmutableInformation(context, restoredData, observabilityOptions(true, 2000L, 5));

            Mockito.when(context.runningJobInfoMap.get(5L))
                    .thenReturn(new JobInfo(500L, firstData), new JobInfo(501L, restoredData));

            Assertions.assertEquals(1, resolve(context.service, 5L).getRetentionMinutes());
            Assertions.assertEquals(5, resolve(context.service, 5L).getRetentionMinutes());
        } finally {
            context.service.shutdown();
        }
    }

    @Test
    public void testStoreIsReusedWithoutConfigChangesAndRecreatedWhenConfigChanges()
            throws Exception {
        TestContext context = newTestContext(Collections.singleton(6L));
        try {
            Data initialData = Mockito.mock(Data.class);
            Data changedData = Mockito.mock(Data.class);
            Data retentionData = Mockito.mock(Data.class);
            JobInfo initialInfo = new JobInfo(600L, initialData);
            JobInfo changedInfo = new JobInfo(601L, changedData);
            JobInfo retentionInfo = new JobInfo(602L, retentionData);
            stubImmutableInformation(context, initialData, observabilityOptions(true, 2000L, 2));
            stubImmutableInformation(context, changedData, observabilityOptions(true, 3000L, 2));
            stubImmutableInformation(context, retentionData, observabilityOptions(true, 3000L, 3));
            Mockito.when(context.runningJobInfoMap.get(6L))
                    .thenReturn(initialInfo, initialInfo, changedInfo, retentionInfo);
            Mockito.when(
                            context.coordinatorService.getRunningJobRawMetrics(
                                    Mockito.anySet(), Mockito.anyLong(), Mockito.any()))
                    .thenReturn(Collections.emptyList());

            collectOnce(context.service);
            Object initialStore = getField(context.service, "jobStores", Map.class).get(6L);
            Assertions.assertNotNull(initialStore);
            collectOnce(context.service);
            Assertions.assertSame(
                    initialStore, getField(context.service, "jobStores", Map.class).get(6L));
            invokeUpdate(
                    initialStore,
                    System.currentTimeMillis(),
                    newJobAggWithQueuePutBlockedNs(601L, 1_000L));

            collectOnce(context.service);
            Object changedStore = getField(context.service, "jobStores", Map.class).get(6L);
            Assertions.assertNotSame(initialStore, changedStore);
            Assertions.assertTrue(
                    ((List<?>) context.service.getJobEdges(6L, 60_000L).get("edges")).isEmpty(),
                    "the recreated store must not contain points from the previous store");

            collectOnce(context.service);
            Assertions.assertNotSame(
                    changedStore, getField(context.service, "jobStores", Map.class).get(6L));
        } finally {
            context.service.shutdown();
        }
    }

    @Test
    public void testConfigCacheUsesInitializationTimestampAndEvictsRemovedJobs() throws Exception {
        TestContext context = newTestContext(Collections.singleton(7L));
        try {
            Data firstData = Mockito.mock(Data.class);
            Data secondData = Mockito.mock(Data.class);
            stubImmutableInformation(context, firstData, observabilityOptions(true, 2000L, 2));
            stubImmutableInformation(context, secondData, observabilityOptions(true, 3000L, 2));
            Mockito.when(context.runningJobInfoMap.get(7L))
                    .thenReturn(
                            new JobInfo(700L, firstData),
                            new JobInfo(700L, firstData),
                            new JobInfo(701L, secondData));

            resolve(context.service, 7L);
            resolve(context.service, 7L);
            resolve(context.service, 7L);

            Mockito.verify(context.serializationService, Mockito.times(1)).toObject(firstData);
            Mockito.verify(context.serializationService, Mockito.times(1)).toObject(secondData);
            Assertions.assertEquals(1, getField(context.service, "configCache", Map.class).size());

            Mockito.when(context.runningJobInfoMap.keySet()).thenReturn(Collections.emptySet());
            collectOnce(context.service);
            Assertions.assertTrue(getField(context.service, "configCache", Map.class).isEmpty());
        } finally {
            context.service.shutdown();
        }
    }

    @Test
    public void testVertexDeltaCalculationUsesBaselineAndNeverNegativeOnReset() throws Exception {
        Object store = newJobStore(5000L, 3);

        long baseMs = 1_000_000_000_000L;
        invokeUpdate(store, baseMs, newJobAggWithSourceReadNs(1L, 100_000_000L));
        Assertions.assertEquals(0L, latestVertexBucketLong(store, 1L, "sourceReadNs"));

        invokeUpdate(store, baseMs + 5000L, newJobAggWithSourceReadNs(1L, 150_000_000L));
        Assertions.assertEquals(50_000_000L, latestVertexBucketLong(store, 1L, "sourceReadNs"));

        // Simulate counter reset/rollback: current sum is smaller than last sum.
        invokeUpdate(store, baseMs + 10_000L, newJobAggWithSourceReadNs(1L, 10_000_000L));
        Assertions.assertEquals(0L, latestVertexBucketLong(store, 1L, "sourceReadNs"));
    }

    @Test
    public void testEdgeDeltaCalculationUsesBaseline() throws Exception {
        Object store = newJobStore(5000L, 3);

        long baseMs = 1_000_000_000_000L;
        long queueId = 101L;
        invokeUpdate(store, baseMs, newJobAggWithQueuePutBlockedNs(queueId, 1_000L));
        Assertions.assertEquals(0L, latestEdgeBucketLong(store, queueId, "emitBlockedNs"));

        invokeUpdate(store, baseMs + 5000L, newJobAggWithQueuePutBlockedNs(queueId, 3_500L));
        Assertions.assertEquals(2_500L, latestEdgeBucketLong(store, queueId, "emitBlockedNs"));
    }

    @Test
    public void testBucketEvictionRemovesExpiredBuckets() throws Exception {
        Object store = newJobStore(5000L, 3);

        long baseMs = 1_000_000_000_000L;
        // Create buckets at 0, 1, 2, 3, 4, 5 minutes.
        for (int i = 0; i <= 5; i++) {
            invokeUpdate(store, baseMs + i * 60_000L, null);
        }

        long nowMs = baseMs + 5 * 60_000L;
        long expireBeforeMs = nowMs - 3L * 60_000L;

        Deque<?> buckets = getField(store, "buckets", Deque.class);
        Object first = buckets.peekFirst();
        Assertions.assertNotNull(first);
        long firstStartMs = getField(first, "bucketStartMs", long.class);
        Assertions.assertTrue(
                firstStartMs >= expireBeforeMs,
                "oldest bucket should not be older than retention window");
        Assertions.assertEquals(4, buckets.size());
    }

    private static Object newJobStore(long bucketMs, int retentionMinutes) throws Exception {
        Class<?> cls = Class.forName(RealtimeMetricsService.class.getName() + "$JobStore");
        Constructor<?> c = cls.getDeclaredConstructor(long.class, int.class);
        c.setAccessible(true);
        return c.newInstance(bucketMs, retentionMinutes);
    }

    private static TestContext newTestContext(Set<Long> runningJobIds) {
        NodeEngineImpl nodeEngine = Mockito.mock(NodeEngineImpl.class);
        HazelcastInstance hazelcastInstance = Mockito.mock(HazelcastInstance.class);
        @SuppressWarnings("unchecked")
        IMap<Long, JobInfo> runningJobInfoMap = Mockito.mock(IMap.class);
        SerializationService serializationService = Mockito.mock(SerializationService.class);
        CoordinatorService coordinatorService = Mockito.mock(CoordinatorService.class);
        Mockito.when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        Mockito.when(nodeEngine.getSerializationService()).thenReturn(serializationService);
        Mockito.doReturn(runningJobInfoMap)
                .when(hazelcastInstance)
                .getMap(Constant.IMAP_RUNNING_JOB_INFO);
        Mockito.when(runningJobInfoMap.keySet()).thenReturn(runningJobIds);
        return new TestContext(
                new RealtimeMetricsService(nodeEngine, coordinatorService),
                coordinatorService,
                runningJobInfoMap,
                serializationService);
    }

    private static void stubCurrentJobInfo(
            TestContext context, long jobId, long initializationTimestamp, Map<String, Object> env)
            throws Exception {
        Data data = Mockito.mock(Data.class);
        Mockito.when(context.runningJobInfoMap.get(jobId))
                .thenReturn(new JobInfo(initializationTimestamp, data));
        stubImmutableInformation(context, data, env);
    }

    private static JobImmutableInformation stubImmutableInformation(
            TestContext context, Data data, Map<String, Object> env) throws Exception {
        JobImmutableInformation immutableInformation = Mockito.mock(JobImmutableInformation.class);
        JobConfig jobConfig = Mockito.mock(JobConfig.class);
        Mockito.when(context.serializationService.<JobImmutableInformation>toObject(data))
                .thenReturn(immutableInformation);
        Mockito.when(immutableInformation.getJobConfig()).thenReturn(jobConfig);
        Mockito.when(jobConfig.getEnvOptions()).thenReturn(env);
        return immutableInformation;
    }

    private static void stubHistory(
            CoordinatorService coordinatorService, long jobId, Map<String, Object> envOptions) {
        JobDAGInfo history = new JobDAGInfo();
        history.setJobId(jobId);
        history.setEnvOptions(envOptions);
        Mockito.when(coordinatorService.getJobInfo(jobId)).thenReturn(history);
    }

    private static Map<String, Object> observabilityOptions(
            boolean enabled, long bucketMs, int retentionMinutes) {
        Map<String, Object> options = new HashMap<>();
        options.put("engine.observability.enabled", enabled);
        options.put("engine.observability.bucket_ms", bucketMs);
        options.put("engine.observability.retention_minutes", retentionMinutes);
        return options;
    }

    private static ObservabilityConfig resolve(RealtimeMetricsService service, long jobId)
            throws Exception {
        Method method =
                RealtimeMetricsService.class.getDeclaredMethod(
                        "resolveObservabilityConfig", long.class);
        method.setAccessible(true);
        return (ObservabilityConfig) method.invoke(service, jobId);
    }

    private static void collectOnce(RealtimeMetricsService service) throws Exception {
        Method method = RealtimeMetricsService.class.getDeclaredMethod("collectOnce");
        method.setAccessible(true);
        try {
            method.invoke(service);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            }
            throw e;
        }
    }

    private static Object newJobAggWithSourceReadNs(long vertexId, long sum) throws Exception {
        Object jobAgg = newJobAgg();
        Object aggById = getField(jobAgg, "sourceReadNs", Object.class);
        invokeAggAdd(aggById, vertexId, sum);
        return jobAgg;
    }

    private static Object newJobAggWithQueuePutBlockedNs(long queueId, long sum) throws Exception {
        Object jobAgg = newJobAgg();
        Object aggById = getField(jobAgg, "queuePutBlockedNs", Object.class);
        invokeAggAdd(aggById, queueId, sum);
        return jobAgg;
    }

    private static Object newJobAgg() throws Exception {
        Class<?> cls = Class.forName(RealtimeMetricsService.class.getName() + "$JobAgg");
        Constructor<?> c = cls.getDeclaredConstructor();
        c.setAccessible(true);
        return c.newInstance();
    }

    private static void invokeUpdate(Object store, long nowMs, Object jobAgg) throws Exception {
        Method m = store.getClass().getDeclaredMethod("update", long.class, jobAggParamClass());
        m.setAccessible(true);
        m.invoke(store, nowMs, jobAgg);
    }

    private static Class<?> jobAggParamClass() throws Exception {
        return Class.forName(RealtimeMetricsService.class.getName() + "$JobAgg");
    }

    private static void invokeAggAdd(Object aggById, long id, long value) throws Exception {
        Method add = aggById.getClass().getDeclaredMethod("add", long.class, long.class);
        add.setAccessible(true);
        add.invoke(aggById, id, value);
    }

    private static long latestVertexBucketLong(Object store, long vertexId, String field)
            throws Exception {
        Deque<?> buckets = getField(store, "buckets", Deque.class);
        Object lastBucket = buckets.peekLast();
        Assertions.assertNotNull(lastBucket);
        Map<?, ?> vertices = getField(lastBucket, "vertices", Map.class);
        Object vertexBucket = vertices.get(vertexId);
        Assertions.assertNotNull(vertexBucket);
        return getField(vertexBucket, field, long.class);
    }

    private static long latestEdgeBucketLong(Object store, long queueId, String field)
            throws Exception {
        Deque<?> buckets = getField(store, "buckets", Deque.class);
        Object lastBucket = buckets.peekLast();
        Assertions.assertNotNull(lastBucket);
        Map<?, ?> edges = getField(lastBucket, "edges", Map.class);
        Object edgeBucket = edges.get(queueId);
        Assertions.assertNotNull(edgeBucket);
        return getField(edgeBucket, field, long.class);
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object target, String field, Class<T> type) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        return (T) f.get(target);
    }

    private static final class TestContext {
        private final RealtimeMetricsService service;
        private final CoordinatorService coordinatorService;
        private final IMap<Long, JobInfo> runningJobInfoMap;
        private final SerializationService serializationService;

        private TestContext(
                RealtimeMetricsService service,
                CoordinatorService coordinatorService,
                IMap<Long, JobInfo> runningJobInfoMap,
                SerializationService serializationService) {
            this.service = service;
            this.coordinatorService = coordinatorService;
            this.runningJobInfoMap = runningJobInfoMap;
            this.serializationService = serializationService;
        }
    }

    private static final class TestAppender extends AbstractAppender {
        private final List<LogEvent> events = new CopyOnWriteArrayList<>();

        private TestAppender() {
            super("realtime-metrics-test-appender", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event);
        }
    }
}
