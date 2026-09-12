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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

/**
 * Verifies cached driver calls preserve concrete-class dispatch, overloads, and JDBC failures. No
 * database connection is required for these driver-dispatch regression tests.
 */
class MppdbReplicationStreamTest {

    // Repeated method names must still dispatch to each concrete driver's implementation.
    @Test
    void testCachedCallsRemainClassSpecificAcrossClose() throws SQLException {
        MppdbReplicationStream stream = new MppdbReplicationStream(null, null, null, null);
        DriverCalls first = new DriverCalls();
        OtherDriverCalls second = new OtherDriverCalls();
        Assertions.assertEquals(1L, stream.invoke(first, "asLong"));
        Assertions.assertEquals(2L, stream.invoke(second, "asLong"));
        Assertions.assertEquals(1L, stream.invoke(first, "asLong"));
        stream.close();
        Assertions.assertEquals(2L, stream.invoke(second, "asLong"));
    }

    // Cache use must not change overload selection or swallow exceptions from the driver.
    @Test
    void testOverloadsAndDriverFailure() throws SQLException {
        MppdbReplicationStream stream = new MppdbReplicationStream(null, null, null, null);
        DriverCalls driver = new DriverCalls();
        Assertions.assertEquals("text", stream.invoke(driver, "withSlotOption", "key", "value"));
        Assertions.assertEquals("number", stream.invoke(driver, "withSlotOption", "key", 1));
        Assertions.assertThrows(SQLException.class, () -> stream.invoke(driver, "missing"));
        Assertions.assertSame(
                driver.failure,
                Assertions.assertThrows(
                        SQLException.class, () -> stream.invoke(driver, "readPending")));
        Assertions.assertSame(
                driver.failure,
                Assertions.assertThrows(
                        SQLException.class, () -> stream.invoke(driver, "readPending")));
    }

    /**
     * Public stand-in for a reflection-compatible JDBC driver with overloaded builder methods. No
     * database connection is required for these driver-dispatch regression tests.
     */
    public static class DriverCalls {

        // Original driver failure whose identity must survive reflective invocation.
        private final SQLException failure = new SQLException("read failed", "08006");

        // Returns a class-specific value through the same no-argument API as a driver LSN.
        public long asLong() {
            return 1;
        }

        // Represents a textual slot option overload.
        public String withSlotOption(String key, String value) {
            return "text";
        }

        // Represents a primitive slot option overload.
        public String withSlotOption(String key, int value) {
            return "number";
        }

        // Models a driver failure reached after a method has entered the cache.
        public Object readPending() throws SQLException {
            throw failure;
        }
    }

    /**
     * Alternate driver class with the same LSN method name and a distinct implementation. No
     * database connection is required for these driver-dispatch regression tests.
     */
    public static class OtherDriverCalls {

        // Returns the alternate driver's value.
        public long asLong() {
            return 2;
        }
    }
}
