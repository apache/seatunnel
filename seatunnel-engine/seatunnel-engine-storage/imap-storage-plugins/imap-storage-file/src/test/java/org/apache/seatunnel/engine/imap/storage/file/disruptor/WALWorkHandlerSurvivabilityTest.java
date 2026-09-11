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

package org.apache.seatunnel.engine.imap.storage.file.disruptor;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALWriter;
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFuture;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFutureCache;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Proves the sole WAL consumer survives a non-{@code IOException} from {@code writer.write()} and
 * fail-closes further APPEND attempts so a possible torn trailer cannot become a mid-file tear.
 */
@EnabledOnOs({LINUX, MAC})
class WALWorkHandlerSurvivabilityTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void nonIoExceptionFromWriteShouldNotKillWorkerAndSubsequentAppendIsFailClosed()
            throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        FileSystem fs = FileSystem.get(conf);
        String parentPath = new Path(tempDir.resolve("wal").toUri()).toString();

        WALWorkHandler handler =
                new WALWorkHandler(
                        fs, FileConfiguration.HDFS, parentPath, new ProtoStuffSerializer());

        AtomicInteger writeCalls = new AtomicInteger();
        WALWriter failingWriter = mock(WALWriter.class);
        doAnswer(
                        invocation -> {
                            writeCalls.getAndIncrement();
                            throw new IllegalStateException("poison write");
                        })
                .when(failingWriter)
                .write(any(IMapFileData.class));
        setWriter(handler, failingWriter);

        long failedRequestId = RequestFutureCache.getRequestId();
        RequestFuture failedFuture = new RequestFuture();
        RequestFutureCache.put(failedRequestId, failedFuture);

        long blockedRequestId = RequestFutureCache.getRequestId();
        RequestFuture blockedFuture = new RequestFuture();
        RequestFutureCache.put(blockedRequestId, blockedFuture);

        IMapFileData data =
                IMapFileData.builder()
                        .deleted(false)
                        .key("k".getBytes())
                        .keyClassName(String.class.getName())
                        .value("v".getBytes())
                        .valueClassName(String.class.getName())
                        .timestamp(System.nanoTime())
                        .build();

        // Must return normally: an escaping RuntimeException would kill the Disruptor worker.
        Assertions.assertDoesNotThrow(
                () ->
                        handler.onEvent(
                                FileWALEvent.builder()
                                        .data(data)
                                        .type(WALEventType.APPEND)
                                        .requestId(failedRequestId)
                                        .build()));
        Assertions.assertTrue(failedFuture.isDone());
        Assertions.assertFalse(failedFuture.get());

        // Fail-closed: second APPEND must complete with false without touching the writer again.
        Assertions.assertDoesNotThrow(
                () ->
                        handler.onEvent(
                                FileWALEvent.builder()
                                        .data(data)
                                        .type(WALEventType.APPEND)
                                        .requestId(blockedRequestId)
                                        .build()));
        Assertions.assertTrue(blockedFuture.isDone());
        Assertions.assertFalse(blockedFuture.get());
        Assertions.assertEquals(1, writeCalls.get());
        verify(failingWriter, times(1)).write(any(IMapFileData.class));
        Assertions.assertTrue(
                handler.isAppendBlockedAfterWriteFailure(),
                "fail-close must be sticky for the handler lifetime");

        RequestFutureCache.remove(failedRequestId);
        RequestFutureCache.remove(blockedRequestId);
    }

    private static void setWriter(WALWorkHandler handler, WALWriter writer) throws Exception {
        Field field = WALWorkHandler.class.getDeclaredField("writer");
        field.setAccessible(true);
        field.set(handler, writer);
    }
}
