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
 * KIND, either express or implied.  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file;

import org.apache.seatunnel.engine.imap.storage.api.ExpirableValue;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.disruptor.WALDisruptor;
import org.apache.seatunnel.engine.imap.storage.file.disruptor.WALEventType;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFutureCache;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;

/**
 * When the WAL ring rejects admission, the storage layer must fail the write and drop the pending
 * future immediately instead of waiting for a completion that cannot arrive.
 */
class IMapFileStorageRingAdmissionTest {

    @Test
    void storeShouldFailFastWhenRingRejectsAdmission() throws Exception {
        IMapFileStorage storage = new IMapFileStorage();
        storage.writDataTimeoutMilliseconds = 60_000L;
        storage.serializer = new ProtoStuffSerializer();

        WALDisruptor walDisruptor = Mockito.mock(WALDisruptor.class);
        Mockito.when(
                        walDisruptor.tryPublish(
                                Mockito.any(IMapFileData.class),
                                Mockito.eq(WALEventType.APPEND),
                                Mockito.anyLong(),
                                Mockito.anyLong()))
                .thenReturn(false);
        injectWalDisruptor(storage, walDisruptor);

        long requestIdBefore = RequestFutureCache.getRequestId();
        Assertions.assertFalse(storage.store("key", "value"));
        Assertions.assertNull(
                RequestFutureCache.get(requestIdBefore + 1),
                "rejected admission must remove its pending future");
    }

    @Test
    void storeAllShouldReportRejectedKeysWithoutWaiting() throws Exception {
        IMapFileStorage storage = new IMapFileStorage();
        storage.writDataTimeoutMilliseconds = 60_000L;
        storage.serializer = new ProtoStuffSerializer();

        WALDisruptor walDisruptor = Mockito.mock(WALDisruptor.class);
        Mockito.when(
                        walDisruptor.tryPublish(
                                Mockito.any(IMapFileData.class),
                                Mockito.eq(WALEventType.APPEND),
                                Mockito.anyLong(),
                                Mockito.anyLong()))
                .thenReturn(false);
        injectWalDisruptor(storage, walDisruptor);

        var failures =
                storage.storeAllWithExpirationTime(
                        Collections.singletonMap("k", new ExpirableValue("v")));
        Assertions.assertEquals(Collections.singleton("k"), failures);
    }

    private static void injectWalDisruptor(IMapFileStorage storage, WALDisruptor walDisruptor)
            throws Exception {
        Field field = IMapFileStorage.class.getDeclaredField("walDisruptor");
        field.setAccessible(true);
        field.set(storage, walDisruptor);
    }
}
