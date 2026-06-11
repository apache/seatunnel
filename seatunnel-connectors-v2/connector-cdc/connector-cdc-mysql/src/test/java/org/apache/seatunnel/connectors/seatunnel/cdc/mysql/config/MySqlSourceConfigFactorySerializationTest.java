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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config;

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
 * {@link MySqlSourceConfigFactory} is part of the serialized job graph (it is shipped to task
 * executors and persisted with the pipeline). It does not declare an explicit {@code
 * serialVersionUID}, so the JVM computes it from the class structure — including non-transient
 * fields and non-private methods. Adding either changes the UID, which makes jobs submitted on a
 * previous version fail to deserialize on a new one during a rolling upgrade ({@code
 * InvalidClassException}).
 *
 * <p>That is why this connector carries {@code int_type_narrowing} through the inherited debezium
 * properties map instead of adding a dedicated field/builder method. These tests guard that
 * decision: the option must survive serialization, and the class structure (hence UID) must stay
 * stable.
 */
class MySqlSourceConfigFactorySerializationTest {

    /** Baseline computed UID of the factory's structure. Must not drift (see class javadoc). */
    private static final long BASELINE_SERIAL_VERSION_UID = -6578851046816898665L;

    @Test
    void serialVersionUidMustNotDrift() {
        long actual =
                ObjectStreamClass.lookup(MySqlSourceConfigFactory.class).getSerialVersionUID();
        Assertions.assertEquals(
                BASELINE_SERIAL_VERSION_UID,
                actual,
                "MySqlSourceConfigFactory serialVersionUID drifted ("
                        + BASELINE_SERIAL_VERSION_UID
                        + " -> "
                        + actual
                        + "). Adding fields/non-private methods to this serialized factory breaks "
                        + "rolling upgrades. Route new options through the dbzProperties map.");
    }

    @Test
    void intTypeNarrowingSurvivesSerializationViaDbzProperties() throws Exception {
        MySqlSourceConfigFactory factory = new MySqlSourceConfigFactory();
        Properties dbz = new Properties();
        dbz.setProperty("int_type_narrowing", "false");
        factory.debeziumProperties(dbz);

        MySqlSourceConfigFactory restored = roundTrip(factory);

        Field f = JdbcSourceConfigFactory.class.getDeclaredField("dbzProperties");
        f.setAccessible(true);
        Properties restoredProps = (Properties) f.get(restored);
        Assertions.assertEquals("false", restoredProps.getProperty("int_type_narrowing"));
    }

    private static MySqlSourceConfigFactory roundTrip(MySqlSourceConfigFactory factory)
            throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(factory);
        }
        try (ObjectInputStream ois =
                new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            return (MySqlSourceConfigFactory) ois.readObject();
        }
    }
}
