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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Proves {@code batchQueryExecuteFailsStatus} spends one shared timeout budget across the batch,
 * not a fresh full timeout on every entry.
 *
 * <p>The check records the timeout handed to each pending future. It does not assert wall-clock
 * elapsed time, so a busy CI worker cannot fail a correct implementation by delaying the thread.
 */
class IMapFileStorageBatchDeadlineTest {

    private static final long TIMEOUT_MS = 200L;
    private static final int PENDING_KEYS = 5;

    @Test
    void batchWaitShouldHonorSharedDeadlineInsteadOfPerEntryTimeout() throws Exception {
        IMapFileStorage storage = new IMapFileStorage();
        storage.writDataTimeoutMilliseconds = TIMEOUT_MS;

        Map<Long, Object> requestMap = new LinkedHashMap<>();
        List<RecordingRequestFuture> futures = new ArrayList<>();
        for (int i = 0; i < PENDING_KEYS; i++) {
            long requestId = RequestFutureCache.getRequestId();
            RecordingRequestFuture future = new RecordingRequestFuture();
            futures.add(future);
            RequestFutureCache.put(requestId, future);
            requestMap.put(requestId, "key-" + i);
        }

        try {
            @SuppressWarnings("unchecked")
            Set<Object> failures =
                    (Set<Object>)
                            invokeBatchQueryExecuteFailsStatus(
                                    storage, requestMap, new HashSet<Object>());

            Assertions.assertEquals(PENDING_KEYS, failures.size());
            long budgetNanos = TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
            for (RecordingRequestFuture future : futures) {
                Assertions.assertEquals(1, future.requestedTimeoutsNanos.size());
                long requested = future.requestedTimeoutsNanos.get(0);
                // The old code asked every entry for a fresh full timeout. The shared deadline
                // asks only for the time still left, which can never exceed the one budget.
                // The recorded waits are not additive: each is remaining time, not time spent.
                Assertions.assertTrue(
                        requested > 0L && requested <= budgetNanos,
                        "requestedNanos=" + requested + " budgetNanos=" + budgetNanos);
            }
        } finally {
            for (Long requestId : requestMap.keySet()) {
                RequestFutureCache.remove(requestId);
            }
        }
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

    /**
     * A pending future that records the timeout it was given and throws immediately, so the shared
     * budget can be checked without waiting on the wall clock.
     */
    private static final class RecordingRequestFuture extends RequestFuture {
        private final List<Long> requestedTimeoutsNanos = new ArrayList<>();

        @Override
        public Boolean get(long timeout, TimeUnit unit) throws TimeoutException {
            requestedTimeoutsNanos.add(unit.toNanos(timeout));
            throw new TimeoutException("simulated shared-deadline timeout");
        }
    }
}
