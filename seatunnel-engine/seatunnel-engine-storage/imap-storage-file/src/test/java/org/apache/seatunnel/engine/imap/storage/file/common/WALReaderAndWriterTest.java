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

package org.apache.seatunnel.engine.imap.storage.file.common;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

import org.apache.seatunnel.engine.imap.storage.api.common.ProtoStuffSerializer;
import org.apache.seatunnel.engine.imap.storage.api.common.Serializer;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

@EnabledOnOs({LINUX, MAC})
public class WALReaderAndWriterTest {

    private static FileSystem FS;
    private static final Path PARENT_PATH = new Path("/tmp/9/");
    private static final Serializer SERIALIZER = new ProtoStuffSerializer();

    @BeforeAll
    public static void init() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem");
        FS = FileSystem.getLocal(conf);
    }

    @Test
    public void testWriterAndReader() throws Exception {
        WALWriter writer = new WALWriter(FS, PARENT_PATH, SERIALIZER);
        IMapFileData data;
        boolean isDelete;
        for (int i = 0; i < 1024; i++) {
            data = IMapFileData.builder()
                .key(SERIALIZER.serialize("key" + i))
                .keyClassName(String.class.getName())
                .value(SERIALIZER.serialize("value" + i))
                .valueClassName(Integer.class.getName())
                .timestamp(System.nanoTime())
                .build();
            if (i % 2 == 0) {
                isDelete = true;
                data.setKey(SERIALIZER.serialize(i));
                data.setKeyClassName(Integer.class.getName());
            } else {
                isDelete = false;
            }
            data.setDeleted(isDelete);

            writer.write(data);
        }
        //update key 511
        data = IMapFileData.builder()
            .key(SERIALIZER.serialize("key" + 511))
            .keyClassName(String.class.getName())
            .value(SERIALIZER.serialize("Kristen"))
            .valueClassName(String.class.getName())
            .deleted(false)
            .timestamp(System.nanoTime())
            .build();
        writer.write(data);
        //delete key 519
        data = IMapFileData.builder()
            .key(SERIALIZER.serialize("key" + 519))
            .keyClassName(String.class.getName())
            .deleted(true)
            .timestamp(System.nanoTime())
            .build();

        writer.write(data);
        writer.close();
        await().atMost(10, java.util.concurrent.TimeUnit.SECONDS).await();

        WALReader reader = new WALReader(FS, new ProtoStuffSerializer());
        Map<Object, Object> result = reader.loadAllData(PARENT_PATH, new HashSet<>());
        Assertions.assertEquals("Kristen", result.get("key511"));
        Assertions.assertEquals(511, result.size());
        Assertions.assertNull(result.get("key519"));

    }

    @AfterAll
    public static void close() throws IOException {
        FS.delete(PARENT_PATH, true);
        FS.close();

    }
}
