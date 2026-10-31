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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Proves {@code batchQueryExecuteFailsStatus} spends one shared timeout budget across the batch,
 * not a fresh full timeout on every entry.
 *
 * <p>The deadline clock is injected and does not advance with the wall clock, so a scheduling pause
 * cannot exhaust the shared budget or fail a correct implementation.
 */
class IMapFileStorageBatchDeadlineTest {

    private static final long TIMEOUT_MS = 200L;
    private static final int PENDING_KEYS = 5;

    @Test
    void batchWaitShouldHonorSharedDeadlineInsteadOfPerEntryTimeout() throws Exception {
        IMapFileStorage storage = new IMapFileStorage();
        storage.writDataTimeoutMilliseconds = TIMEOUT_MS;
        AtomicLong clock = new AtomicLong(1_000_000_000L);
        storage.setNanoTimeSource(clock::get);

        Map<Long, Object> requestMap = new LinkedHashMap<>();
        List<RecordingRequestFuture> futures = new ArrayList<>();
        for (int i = 0; i < PENDING_KEYS; i++) {
            long requestId = RequestFutureCache.getRequestId();
            RecordingRequestFuture future = new RecordingRequestFuture(clock, false);
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
            long expected = budgetNanos;
            for (RecordingRequestFuture future : futures) {
                Assertions.assertEquals(1, future.requestedTimeoutsNanos.size());
                // The injected clock does not move in this case, so every entry is asked for the
                // same remaining budget. A scheduling pause cannot change that.
                Assertions.assertEquals(expected, future.requestedTimeoutsNanos.get(0).longValue());
            }
            Assertions.assertEquals(1_000_000_000L, clock.get());
        } finally {
            for (Long requestId : requestMap.keySet()) {
                RequestFutureCache.remove(requestId);
            }
        }
    }

    @Test
    void batchWaitShouldStopAskingOnceTheInjectedClockPassesTheDeadline() throws Exception {
        IMapFileStorage storage = new IMapFileStorage();
        storage.writDataTimeoutMilliseconds = TIMEOUT_MS;
        AtomicLong clock = new AtomicLong(0L);
        storage.setNanoTimeSource(clock::get);

        Map<Long, Object> requestMap = new LinkedHashMap<>();
        List<RecordingRequestFuture> futures = new ArrayList<>();
        for (int i = 0; i < PENDING_KEYS; i++) {
            long requestId = RequestFutureCache.getRequestId();
            RecordingRequestFuture future = new RecordingRequestFuture(clock, true);
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
            // The first wait consumes the whole budget on the injected clock. Later entries must
            // not be asked for another full timeout, which is what the old per-entry wait did.
            Assertions.assertEquals(
                    TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS),
                    futures.get(0).requestedTimeoutsNanos.get(0).longValue());
            for (int i = 1; i < futures.size(); i++) {
                Assertions.assertTrue(
                        futures.get(i).requestedTimeoutsNanos.isEmpty(),
                        "entry " + i + " was waited on after the shared deadline expired");
            }
            Assertions.assertEquals(
                    TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS),
                    clock.get(),
                    "the injected clock must be the only thing that spends the shared budget");
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
     * A pending future that records the timeout it was given, spends that time on the injected
     * clock, and throws immediately. Wall time does not move, so a scheduling pause cannot change
     * the result.
     */
    private static final class RecordingRequestFuture extends RequestFuture {
        private final AtomicLong clock;
        private final boolean spendRequestedTimeout;
        private final List<Long> requestedTimeoutsNanos = new ArrayList<>();

        private RecordingRequestFuture(AtomicLong clock, boolean spendRequestedTimeout) {
            this.clock = clock;
            this.spendRequestedTimeout = spendRequestedTimeout;
        }

        @Override
        public Boolean get(long timeout, TimeUnit unit) throws TimeoutException {
            long timeoutNanos = unit.toNanos(timeout);
            requestedTimeoutsNanos.add(timeoutNanos);
            if (spendRequestedTimeout) {
                clock.addAndGet(timeoutNanos);
            }
            throw new TimeoutException("simulated shared-deadline timeout");
        }
    }
}
