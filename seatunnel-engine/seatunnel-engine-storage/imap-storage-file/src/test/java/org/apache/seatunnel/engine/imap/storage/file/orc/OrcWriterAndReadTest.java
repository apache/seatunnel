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

package org.apache.seatunnel.engine.imap.storage.file.orc;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

import org.apache.seatunnel.engine.imap.storage.api.common.ProtoStuffSerializer;
import org.apache.seatunnel.engine.imap.storage.api.common.Serializer;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapData;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import java.io.IOException;
import java.util.List;

@EnabledOnOs({LINUX, MAC})
public class OrcWriterAndReadTest {

    private static Configuration CONF = new Configuration();

    private static Path PATH = new Path("/tmp/orc/OrcWriterAndReadTest1.orc");

    private static Serializer SERIALIZER = new ProtoStuffSerializer();

    @BeforeAll
    public static void beforeAll() throws IOException {
        OrcWriter writer = new OrcWriter(PATH, CONF);
        IMapFileData data;
        for (int i = 0; i < 100; i++) {
            String key = "key" + i;
            Long value = System.currentTimeMillis();

            data = IMapFileData.builder()
                .deleted(false)
                .key(SERIALIZER.serialize(key))
                .keyClassName(String.class.getName())
                .value(SERIALIZER.serialize(value))
                .valueClassName(String.class.getName())
                .timestamp(System.nanoTime())
                .build();
            writer.write(data);
        }
        writer.close();
    }

    @Test
    void testRead() throws IOException {
        OrcReader reader = new OrcReader(PATH, CONF);
        List<IMapData> datas = reader.queryAll();
        datas.forEach(data -> {
            try {
                SERIALIZER.deserialize(data.getKey(), String.class);
                SERIALIZER.deserialize(data.getValue(), Long.class);
            } catch (IOException e) {
                Assertions.fail(e);
            }
        });
        Assertions.assertEquals(100, datas.size());
    }

    @AfterAll
    public static void afterAll() throws IOException {
        Assertions.assertTrue(PATH.getFileSystem(CONF).delete(PATH, true));
    }
}
