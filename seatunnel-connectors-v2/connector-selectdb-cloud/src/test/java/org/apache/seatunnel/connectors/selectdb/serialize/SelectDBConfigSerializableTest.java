/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.selectdb.serialize;

import org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Properties;

public class SelectDBConfigSerializableTest {

    @Test
    void testSelectDBConfigSerializable() throws Exception {
        SelectDBConfig config = new SelectDBConfig();
        config.setLoadUrl("localhost:8080");
        config.setJdbcUrl("localhost:9030");
        config.setClusterName("cluster");
        config.setUsername("user");
        config.setPassword("pwd");
        config.setTableIdentifier("db.table");
        config.setEnableDelete(true);
        config.setLabelPrefix("label");
        config.setEnable2PC(true);
        config.setMaxRetries(3);
        config.setBufferSize(1024);
        config.setBufferCount(2);
        config.setFlushQueueSize(10);
        Properties stageLoadProps = new Properties();
        stageLoadProps.setProperty("file.type", "json");
        config.setStageLoadProps(stageLoadProps);

        SelectDBConfig deserialized = roundTrip(config);

        Assertions.assertEquals(config.getLoadUrl(), deserialized.getLoadUrl());
        Assertions.assertEquals(config.getJdbcUrl(), deserialized.getJdbcUrl());
        Assertions.assertEquals(config.getClusterName(), deserialized.getClusterName());
        Assertions.assertEquals(config.getUsername(), deserialized.getUsername());
        Assertions.assertEquals(config.getPassword(), deserialized.getPassword());
        Assertions.assertEquals(config.getTableIdentifier(), deserialized.getTableIdentifier());
        Assertions.assertEquals(config.getEnableDelete(), deserialized.getEnableDelete());
        Assertions.assertEquals(config.getLabelPrefix(), deserialized.getLabelPrefix());
        Assertions.assertEquals(config.isEnable2PC(), deserialized.isEnable2PC());
        Assertions.assertEquals(config.getMaxRetries(), deserialized.getMaxRetries());
        Assertions.assertEquals(config.getBufferSize(), deserialized.getBufferSize());
        Assertions.assertEquals(config.getBufferCount(), deserialized.getBufferCount());
        Assertions.assertEquals(config.getFlushQueueSize(), deserialized.getFlushQueueSize());
        Assertions.assertEquals(
                config.getStageLoadProps().getProperty("file.type"),
                deserialized.getStageLoadProps().getProperty("file.type"));
    }

    @Test
    void testSelectDBConfigSerializableWithNullStageLoadProps() throws Exception {
        SelectDBConfig config = new SelectDBConfig();
        config.setLoadUrl("localhost:8080");
        config.setJdbcUrl("localhost:9030");
        config.setUsername("user");
        config.setPassword("pwd");
        // stageLoadProps not set, keep it null

        SelectDBConfig deserialized = roundTrip(config);

        Assertions.assertEquals(config.getLoadUrl(), deserialized.getLoadUrl());
        Assertions.assertEquals(config.getJdbcUrl(), deserialized.getJdbcUrl());
        Assertions.assertEquals(config.getUsername(), deserialized.getUsername());
        Assertions.assertEquals(config.getPassword(), deserialized.getPassword());
        Assertions.assertNull(deserialized.getStageLoadProps());
    }

    @Test
    void testSelectDBConfigSerializableWithEmptyStageLoadProps() throws Exception {
        SelectDBConfig config = new SelectDBConfig();
        config.setLoadUrl("localhost:8080");
        config.setStageLoadProps(new Properties());

        SelectDBConfig deserialized = roundTrip(config);

        Assertions.assertEquals(config.getLoadUrl(), deserialized.getLoadUrl());
        Assertions.assertNotNull(deserialized.getStageLoadProps());
        Assertions.assertTrue(deserialized.getStageLoadProps().isEmpty());
    }

    private static SelectDBConfig roundTrip(SelectDBConfig config) throws Exception {
        byte[] serialized;
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream =
                        new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(config);
            objectOutputStream.flush();
            serialized = byteArrayOutputStream.toByteArray();
        }

        try (ObjectInputStream objectInputStream =
                new ObjectInputStream(new ByteArrayInputStream(serialized))) {
            return (SelectDBConfig) objectInputStream.readObject();
        }
    }
}
