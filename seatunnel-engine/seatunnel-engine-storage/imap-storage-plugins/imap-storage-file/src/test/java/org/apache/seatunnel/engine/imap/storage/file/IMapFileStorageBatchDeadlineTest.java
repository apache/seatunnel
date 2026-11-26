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

import org.apache.seatunnel.engine.imap.storage.api.ExpirableValue;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.disruptor.WALDisruptor;
import org.apache.seatunnel.engine.imap.storage.file.disruptor.WALEventType;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Proves batch {@link IMapFileStorage#storeAll(Map)} / {@link IMapFileStorage#deleteAll} honor one
 * shared timeout budget instead of a fresh full timeout on every entry.
 */
class IMapFileStorageBatchDeadlineTest {

    private static final long TIMEOUT_MS = 50L;
    private static final int PENDING_KEYS = 5;

    @Test
    void storeAllShouldSpendOneSharedTimeoutAcrossBatch() throws Exception {
        AtomicLong clockNanos = new AtomicLong(0L);
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
        IMapFileStorage storage = storageWithMockWal(clockNanos, timeoutNanos);
        Map<Object, Object> batch = new LinkedHashMap<>();
        for (int index = 0; index < PENDING_KEYS; index++) {
            batch.put("key-" + index, "value-" + index);
        }

        Set<Object> failures = storage.storeAll(batch);

        Assertions.assertEquals(batch.keySet(), failures);
        ArgumentCaptor<Long> admissionBudgetCaptor = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(storageWal(storage), Mockito.times(PENDING_KEYS))
                .tryPublish(
                        Mockito.any(IMapFileData.class),
                        Mockito.eq(WALEventType.APPEND),
                        Mockito.anyLong(),
                        admissionBudgetCaptor.capture());
        List<Long> admissionBudgets = admissionBudgetCaptor.getAllValues();
        for (Long budget : admissionBudgets) {
            Assertions.assertEquals(
                    timeoutNanos,
                    budget.longValue(),
                    "each admission should use the same remaining write timeout");
        }
    }

    @Test
    void deleteAllShouldSpendOneSharedTimeoutAcrossBatch() throws Exception {
        AtomicLong clockNanos = new AtomicLong(0L);
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
        IMapFileStorage storage = storageWithMockWal(clockNanos, timeoutNanos);
        Map<Object, ExpirableValue> values = new LinkedHashMap<>();
        for (int index = 0; index < PENDING_KEYS; index++) {
            values.put("key-" + index, new ExpirableValue("value-" + index));
        }

        Set<Object> failures = storage.deleteAll(values.keySet());

        Assertions.assertEquals(values.keySet(), failures);
    }

    private static IMapFileStorage storageWithMockWal(AtomicLong clockNanos, long timeoutNanos)
            throws Exception {
        IMapFileStorage storage = new IMapFileStorage();
        storage.writDataTimeoutMilliseconds = TIMEOUT_MS;
        storage.serializer = new ProtoStuffSerializer();
        storage.setNanoTimeSource(clockNanos::get);

        WALDisruptor walDisruptor = Mockito.mock(WALDisruptor.class);
        AtomicInteger publishCount = new AtomicInteger();
        Mockito.when(
                        walDisruptor.tryPublish(
                                Mockito.any(IMapFileData.class),
                                Mockito.eq(WALEventType.APPEND),
                                Mockito.anyLong(),
                                Mockito.anyLong()))
                .thenAnswer(
                        invocation -> {
                            if (publishCount.incrementAndGet() >= PENDING_KEYS) {
                                // Exhaust the shared budget before the batch wait loop so harvest
                                // uses get(0) and fails fast without wall-clock sleeps.
                                clockNanos.set(timeoutNanos);
                            }
                            return true;
                        });
        injectWalDisruptor(storage, walDisruptor);
        return storage;
    }

    private static WALDisruptor storageWal(IMapFileStorage storage) throws Exception {
        Field field = IMapFileStorage.class.getDeclaredField("walDisruptor");
        field.setAccessible(true);
        return (WALDisruptor) field.get(storage);
    }

    private static void injectWalDisruptor(IMapFileStorage storage, WALDisruptor walDisruptor)
            throws Exception {
        Field field = IMapFileStorage.class.getDeclaredField("walDisruptor");
        field.setAccessible(true);
        field.set(storage, walDisruptor);
    }
}
