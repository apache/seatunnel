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
import org.apache.seatunnel.engine.imap.storage.file.disruptor.BlockingWalWorkHandler;
import org.apache.seatunnel.engine.imap.storage.file.disruptor.WALDisruptor;
import org.apache.seatunnel.engine.imap.storage.file.disruptor.WALEventType;
import org.apache.seatunnel.engine.imap.storage.file.disruptor.WALWorkHandler;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFutureCache;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

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

    @EnabledOnOs({LINUX, MAC})
    @Test
    void storeShouldFailWithinWatchdogWhenInjectedRingIsSaturated() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        String parent = "/tmp/IMapFileStorageRingAdmissionTest-" + System.nanoTime() + "/";
        FileSystem fs = FileSystem.get(conf);

        WALWorkHandler delegate =
                new WALWorkHandler(
                        fs,
                        org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration.HDFS,
                        parent,
                        new ProtoStuffSerializer());
        BlockingWalWorkHandler blockingHandler = new BlockingWalWorkHandler(delegate);
        WALDisruptor walDisruptor =
                new WALDisruptor(
                        fs,
                        org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration.HDFS,
                        parent,
                        new ProtoStuffSerializer(),
                        4,
                        blockingHandler);
        try {
            IMapFileStorage storage = new IMapFileStorage();
            storage.writDataTimeoutMilliseconds = 300L;
            storage.serializer = new ProtoStuffSerializer();
            injectWalDisruptor(storage, walDisruptor);

            for (int index = 0; index < 4; index++) {
                long requestId = RequestFutureCache.getRequestId();
                Assertions.assertTrue(
                        walDisruptor.tryPublish(
                                sampleData("seed-" + index),
                                WALEventType.APPEND,
                                requestId,
                                java.util.concurrent.TimeUnit.SECONDS.toNanos(5)),
                        "should saturate the ring without waiting for completion");
            }

            long requestIdBefore = RequestFutureCache.getRequestId();
            long started = System.nanoTime();
            Assertions.assertFalse(storage.store("overflow", "v"));
            long elapsedMs =
                    java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            Assertions.assertTrue(elapsedMs <= 600L, "store should fail inside the write timeout");
            Assertions.assertNull(
                    RequestFutureCache.get(requestIdBefore + 1),
                    "rejected admission must remove its pending future");
        } finally {
            blockingHandler.release();
            walDisruptor.close();
            fs.delete(new Path(parent), true);
        }
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

    private static IMapFileData sampleData(String key) {
        return IMapFileData.builder()
                .deleted(false)
                .key(key.getBytes())
                .keyClassName(String.class.getName())
                .value("v".getBytes())
                .valueClassName(String.class.getName())
                .timestamp(System.currentTimeMillis())
                .build();
    }
}
