/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file;

import org.apache.seatunnel.engine.imap.storage.file.future.RequestFuture;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFutureCache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Proves {@code batchQueryExecuteFailsStatus} uses one shared deadline across the batch (~1×
 * timeout total), not a per-entry wait of N× timeout.
 */
class IMapFileStorageBatchDeadlineTest {

    private static final long TIMEOUT_MS = 200L;
    private static final int PENDING_KEYS = 5;

    @Test
    void batchWaitShouldHonorSharedDeadlineInsteadOfPerEntryTimeout() throws Exception {
        IMapFileStorage storage = new IMapFileStorage();
        storage.writDataTimeoutMilliseconds = TIMEOUT_MS;

        Map<Long, Object> requestMap = new LinkedHashMap<>();
        for (int i = 0; i < PENDING_KEYS; i++) {
            long requestId = RequestFutureCache.getRequestId();
            // Never completed: each get() would wait until the shared deadline elapses.
            RequestFutureCache.put(requestId, new RequestFuture());
            requestMap.put(requestId, "key-" + i);
        }

        long startedNanos = System.nanoTime();
        @SuppressWarnings("unchecked")
        Set<Object> failures =
                (Set<Object>)
                        invokeBatchQueryExecuteFailsStatus(
                                storage, requestMap, new HashSet<Object>());
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);

        Assertions.assertEquals(PENDING_KEYS, failures.size());
        // Shared deadline ~TIMEOUT_MS. Allow generous scheduling slack, but stay well under
        // N × TIMEOUT_MS (the pre-fix per-entry behavior).
        Assertions.assertTrue(
                elapsedMs < TIMEOUT_MS * PENDING_KEYS,
                "elapsedMs=" + elapsedMs + " should be < N×timeout=" + (TIMEOUT_MS * PENDING_KEYS));
        Assertions.assertTrue(
                elapsedMs < TIMEOUT_MS * 3,
                "elapsedMs=" + elapsedMs + " should stay near 1×timeout=" + TIMEOUT_MS);
    }

    private static Object invokeBatchQueryExecuteFailsStatus(
            IMapFileStorage storage, Map<Long, Object> requestMap, Set<Object> failures)
            throws Exception {
        Method method =
                IMapFileStorage.class.getDeclaredMethod(
                        "batchQueryExecuteFailsStatus", Map.class, Set.class);
        method.setAccessible(true);
        return method.invoke(storage, requestMap, failures);
    }
}
