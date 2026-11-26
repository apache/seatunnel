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
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFutureCache;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

/** Regression tests for bounded ring admission ({@linkplain WALDisruptor#tryPublish}). */
@EnabledOnOs({LINUX, MAC})
class WALDisruptorBoundedAdmissionTest {

    private static final String FILEPATH = "/tmp/WALDisruptorBoundedAdmissionTest/";

    private static final int SATURATED_RING_SIZE = 4;

    private static final Configuration CONF;

    static {
        CONF = new Configuration();
        CONF.set("fs.defaultFS", "file:///");
        CONF.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    }

    private FileSystem fs;
    private WALDisruptor disruptor;
    private BlockingWalWorkHandler blockingHandler;

    @AfterEach
    void tearDown() throws IOException {
        if (blockingHandler != null) {
            blockingHandler.release();
        }
        if (disruptor != null) {
            disruptor.close();
        }
        if (fs != null) {
            fs.delete(new Path(FILEPATH), true);
        }
    }

    @Test
    void boundedTryPublishShouldAdmitWithSufficientWait() throws IOException {
        fs = FileSystem.get(CONF);
        disruptor =
                new WALDisruptor(fs, FileConfiguration.HDFS, FILEPATH, new ProtoStuffSerializer());
        IMapFileData data = sampleData("key");

        Assertions.assertTrue(
                disruptor.tryPublish(
                        data,
                        WALEventType.APPEND,
                        1L,
                        java.util.concurrent.TimeUnit.SECONDS.toNanos(5)),
                "an open ring should admit an event when the wait budget has not expired");
    }

    @Test
    void boundedTryPublishShouldRejectAfterClose() throws IOException {
        fs = FileSystem.get(CONF);
        disruptor =
                new WALDisruptor(fs, FileConfiguration.HDFS, FILEPATH, new ProtoStuffSerializer());
        disruptor.close();
        IMapFileData data = sampleData("key");

        Assertions.assertFalse(
                disruptor.tryPublish(data, WALEventType.APPEND, 2L, Long.MAX_VALUE),
                "closed ring must not accept steady-state appends");
    }

    @Test
    void boundedTryPublishShouldRejectWhenRingIsSaturatedBeforeDeadline() throws IOException {
        fs = FileSystem.get(CONF);
        WALWorkHandler delegate =
                new WALWorkHandler(
                        fs, FileConfiguration.HDFS, FILEPATH, new ProtoStuffSerializer());
        blockingHandler = new BlockingWalWorkHandler(delegate);
        disruptor =
                new WALDisruptor(
                        fs,
                        FileConfiguration.HDFS,
                        FILEPATH,
                        new ProtoStuffSerializer(),
                        SATURATED_RING_SIZE,
                        blockingHandler);

        try {
            long watchdogNanos = TimeUnit.MILLISECONDS.toNanos(250);
            for (int index = 0; index < SATURATED_RING_SIZE; index++) {
                long requestId = RequestFutureCache.getRequestId();
                Assertions.assertTrue(
                        disruptor.tryPublish(
                                sampleData("k-" + index),
                                WALEventType.APPEND,
                                requestId,
                                TimeUnit.SECONDS.toNanos(5)),
                        "ring should admit events until it is full");
            }

            long rejectedId = RequestFutureCache.getRequestId();
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<Boolean> admission =
                        executor.submit(
                                () ->
                                        disruptor.tryPublish(
                                                sampleData("overflow"),
                                                WALEventType.APPEND,
                                                rejectedId,
                                                watchdogNanos));
                Assertions.assertFalse(
                        admission.get(10, TimeUnit.SECONDS),
                        "a saturated ring must reject admission before the watchdog elapses");
            } catch (TimeoutException | ExecutionException | InterruptedException publishStall) {
                Assertions.fail(
                        "saturated-ring admission must not block the test thread without an outer watchdog",
                        publishStall);
            } finally {
                executor.shutdownNow();
            }
            Assertions.assertNull(
                    RequestFutureCache.get(rejectedId),
                    "rejected admission must not leave a dangling future");
        } finally {
            blockingHandler.release();
            blockingHandler = null;
        }
    }

    @Test
    void boundedTryPublishShouldRejectWhenWaitingThreadIsInterrupted() throws Exception {
        fs = FileSystem.get(CONF);
        WALWorkHandler delegate =
                new WALWorkHandler(
                        fs, FileConfiguration.HDFS, FILEPATH, new ProtoStuffSerializer());
        blockingHandler = new BlockingWalWorkHandler(delegate);
        disruptor =
                new WALDisruptor(
                        fs,
                        FileConfiguration.HDFS,
                        FILEPATH,
                        new ProtoStuffSerializer(),
                        SATURATED_RING_SIZE,
                        blockingHandler);

        Thread publisher = null;
        try {
            for (int index = 0; index < SATURATED_RING_SIZE; index++) {
                long requestId = RequestFutureCache.getRequestId();
                Assertions.assertTrue(
                        disruptor.tryPublish(
                                sampleData("fill-" + index),
                                WALEventType.APPEND,
                                requestId,
                                TimeUnit.SECONDS.toNanos(5)));
            }

            long overflowId = RequestFutureCache.getRequestId();
            AtomicBoolean admitted = new AtomicBoolean(true);
            AtomicBoolean interruptPreservedOnExit = new AtomicBoolean(false);
            CountDownLatch publishStarted = new CountDownLatch(1);
            publisher =
                    new Thread(
                            () -> {
                                publishStarted.countDown();
                                admitted.set(
                                        disruptor.tryPublish(
                                                sampleData("interrupted"),
                                                WALEventType.APPEND,
                                                overflowId,
                                                TimeUnit.SECONDS.toNanos(30)));
                                interruptPreservedOnExit.set(
                                        Thread.currentThread().isInterrupted());
                            },
                            "wal-saturated-publisher");
            publisher.start();
            Assertions.assertTrue(publishStarted.await(5, TimeUnit.SECONDS));
            Thread.sleep(50);
            publisher.interrupt();
            publisher.join(TimeUnit.SECONDS.toMillis(10));

            Assertions.assertFalse(admitted.get(), "interrupted admission must fail");
            Assertions.assertTrue(
                    interruptPreservedOnExit.get(),
                    "tryPublish must return without clearing the caller's interrupt status");
            Assertions.assertNull(RequestFutureCache.get(overflowId));
        } finally {
            if (publisher != null && publisher.isAlive()) {
                publisher.interrupt();
                publisher.join(TimeUnit.SECONDS.toMillis(10));
            }
            if (blockingHandler != null) {
                blockingHandler.release();
                blockingHandler = null;
            }
        }
    }

    private static IMapFileData sampleData(String key) {
        return IMapFileData.builder()
                .deleted(false)
                .key(key.getBytes())
                .keyClassName(String.class.getName())
                .value("value".getBytes())
                .valueClassName(String.class.getName())
                .timestamp(System.currentTimeMillis())
                .build();
    }
}
