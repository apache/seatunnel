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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.config;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Field;
import java.util.Properties;

/**
 * {@link MariaDbSourceConfigFactory} is part of the serialized job graph (it is shipped to task
 * executors and persisted with the pipeline). It explicitly declares {@code serialVersionUID = 1L}.
 *
 * <p>This test guards that the option {@code int_type_narrowing} survives serialization via
 * Debezium properties map.
 */
class MariaDbSourceConfigFactorySerializationTest {

    private static final long BASELINE_SERIAL_VERSION_UID = 1L;

    @Test
    void serialVersionUidMustNotDrift() {
        long actual =
                ObjectStreamClass.lookup(MariaDbSourceConfigFactory.class).getSerialVersionUID();
        Assertions.assertEquals(
                BASELINE_SERIAL_VERSION_UID,
                actual,
                "MariaDbSourceConfigFactory serialVersionUID drifted ("
                        + BASELINE_SERIAL_VERSION_UID
                        + " -> "
                        + actual
                        + ").");
    }

    @Test
    void intTypeNarrowingSurvivesSerializationViaDbzProperties() throws Exception {
        MariaDbSourceConfigFactory factory = new MariaDbSourceConfigFactory();
        Properties dbz = new Properties();
        dbz.setProperty("int_type_narrowing", "false");
        factory.debeziumProperties(dbz);

        MariaDbSourceConfigFactory restored = roundTrip(factory);

        Field f = JdbcSourceConfigFactory.class.getDeclaredField("dbzProperties");
        f.setAccessible(true);
        Properties restoredProps = (Properties) f.get(restored);
        Assertions.assertEquals("false", restoredProps.getProperty("int_type_narrowing"));
    }

    private static MariaDbSourceConfigFactory roundTrip(MariaDbSourceConfigFactory factory)
            throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(factory);
        }
        try (ObjectInputStream ois =
                new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            return (MariaDbSourceConfigFactory) ois.readObject();
        }
    }
}
