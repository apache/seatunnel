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
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.HdfsWriter;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Proves the sole WAL consumer survives write-path failures (including flush/sync) and fail-closes
 * further APPEND attempts so a possible torn trailer cannot become a mid-file tear.
 */
@EnabledOnOs({LINUX, MAC})
class WALWorkHandlerSurvivabilityTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void nonIoExceptionFromWriteShouldNotKillWorkerAndSubsequentAppendIsFailClosed()
            throws Exception {
        WALWorkHandler handler = newHandler("wal-non-io");

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

        assertFailCloseAfterFirstWriteFailure(handler, failingWriter, writeCalls);
    }

    /**
     * Pins fail-close when the durable sync path fails: {@link HdfsWriter#write} appends bytes then
     * always ends in {@link HdfsWriter#flush()} ({@code hsync}). A throw from {@code hsync} must
     * trip the sticky flag the same way as a throw from the append itself — not only via the
     * current {@code catch (Exception)} + call-structure argument.
     */
    @Test
    void flushFailureFromHdfsWriterShouldFailCloseSubsequentAppend() throws Exception {
        WALWorkHandler handler = newHandler("wal-flush");

        FSDataOutputStream out = mock(FSDataOutputStream.class);
        when(out.getWrappedStream()).thenReturn(mock(OutputStream.class));
        // Append path succeeds; only the sync path fails.
        doAnswer(invocation -> null).when(out).write(any(byte[].class), anyInt(), anyInt());
        doThrow(new IOException("hsync failed")).when(out).hsync();

        HdfsWriter hdfsWriter = new HdfsWriter();
        setField(hdfsWriter, "out", out);
        setField(hdfsWriter, "serializer", new ProtoStuffSerializer());

        AtomicInteger writeCalls = new AtomicInteger();
        WALWriter flushFailingWriter = mock(WALWriter.class);
        doAnswer(
                        invocation -> {
                            writeCalls.getAndIncrement();
                            hdfsWriter.write(invocation.getArgument(0));
                            return null;
                        })
                .when(flushFailingWriter)
                .write(any(IMapFileData.class));
        setWriter(handler, flushFailingWriter);

        assertFailCloseAfterFirstWriteFailure(handler, flushFailingWriter, writeCalls);
        // Failure originated from flush/sync, after bytes were handed to the stream.
        verify(out, times(1)).hsync();
    }

    private WALWorkHandler newHandler(String walDirName) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        FileSystem fs = FileSystem.get(conf);
        String parentPath = new Path(tempDir.resolve(walDirName).toUri()).toString();
        return new WALWorkHandler(
                fs, FileConfiguration.HDFS, parentPath, new ProtoStuffSerializer());
    }

    private static void assertFailCloseAfterFirstWriteFailure(
            WALWorkHandler handler, WALWriter failingWriter, AtomicInteger writeCalls)
            throws Exception {
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

        // Must return normally: an escaping exception would kill the Disruptor worker.
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
        setField(handler, "writer", writer);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
