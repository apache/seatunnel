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
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import java.io.IOException;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

/** Regression tests for bounded ring admission ({@linkplain WALDisruptor#tryPublish}). */
@EnabledOnOs({LINUX, MAC})
class WALDisruptorBoundedAdmissionTest {

    private static final String FILEPATH = "/tmp/WALDisruptorBoundedAdmissionTest/";

    private static final Configuration CONF;

    static {
        CONF = new Configuration();
        CONF.set("fs.defaultFS", "file:///");
        CONF.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    }

    private FileSystem fs;
    private WALDisruptor disruptor;

    @AfterEach
    void tearDown() throws IOException {
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
