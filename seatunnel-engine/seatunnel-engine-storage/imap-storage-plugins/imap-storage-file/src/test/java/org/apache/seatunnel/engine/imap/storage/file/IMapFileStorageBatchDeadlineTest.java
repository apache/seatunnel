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
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Proves batch {@link IMapFileStorage#storeAll(Map)} / {@link IMapFileStorage#deleteAll} honor one
 * shared timeout budget instead of a fresh full timeout on every entry.
 */
class IMapFileStorageBatchDeadlineTest {

    private static final long TIMEOUT_MS = 50L;
    private static final int PENDING_KEYS = 5;

    @Test
    void storeAllShouldSpendOneSharedTimeoutAcrossBatch() throws Exception {
        IMapFileStorage storage = storageWithMockWal();
        Map<Object, Object> batch = new LinkedHashMap<>();
        for (int index = 0; index < PENDING_KEYS; index++) {
            batch.put("key-" + index, "value-" + index);
        }

        long started = System.nanoTime();
        Set<Object> failures = storage.storeAll(batch);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        Assertions.assertEquals(batch.keySet(), failures);
        Assertions.assertTrue(
                elapsedMs < TIMEOUT_MS * PENDING_KEYS,
                "storeAll must not spend a full timeout per entry (elapsed "
                        + elapsedMs
                        + " ms for "
                        + PENDING_KEYS
                        + " keys)");
        Assertions.assertTrue(
                elapsedMs >= TIMEOUT_MS - 15L,
                "storeAll should consume roughly one shared timeout budget");
    }

    @Test
    void deleteAllShouldSpendOneSharedTimeoutAcrossBatch() throws Exception {
        IMapFileStorage storage = storageWithMockWal();
        Map<Object, ExpirableValue> values = new LinkedHashMap<>();
        for (int index = 0; index < PENDING_KEYS; index++) {
            values.put("key-" + index, new ExpirableValue("value-" + index));
        }

        long started = System.nanoTime();
        Set<Object> failures = storage.deleteAll(values.keySet());
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        Assertions.assertEquals(values.keySet(), failures);
        Assertions.assertTrue(
                elapsedMs < TIMEOUT_MS * PENDING_KEYS,
                "deleteAll must not spend a full timeout per key");
    }

    private static IMapFileStorage storageWithMockWal() throws Exception {
        IMapFileStorage storage = new IMapFileStorage();
        storage.writDataTimeoutMilliseconds = TIMEOUT_MS;
        storage.serializer = new ProtoStuffSerializer();

        WALDisruptor walDisruptor = Mockito.mock(WALDisruptor.class);
        Mockito.when(
                        walDisruptor.tryPublish(
                                Mockito.any(IMapFileData.class),
                                Mockito.eq(WALEventType.APPEND),
                                Mockito.anyLong(),
                                Mockito.anyLong()))
                .thenAnswer(invocation -> true);
        injectWalDisruptor(storage, walDisruptor);
        return storage;
    }

    private static void injectWalDisruptor(IMapFileStorage storage, WALDisruptor walDisruptor)
            throws Exception {
        Field field = IMapFileStorage.class.getDeclaredField("walDisruptor");
        field.setAccessible(true);
        field.set(storage, walDisruptor);
    }
}
