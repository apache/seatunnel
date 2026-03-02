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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Deque;
import java.util.Map;

public class RealtimeMetricsServiceTest {

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
}
