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

package org.apache.seatunnel.connectors.seatunnel.hbase;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.hbase.sink.HbaseSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.hbase.source.HbaseSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests factory option rules, including the half-open {@code [start_timestamp, end_timestamp)}
 * range.
 */
public class HbaseFactoryTest {

    @Test
    public void optionRuleTest() {
        Assertions.assertNotNull((new HbaseSinkFactory()).optionRule());
        Assertions.assertNotNull((new HbaseSourceFactory()).optionRule());
    }

    @Test
    void testValidTimestampRanges() {
        Assertions.assertDoesNotThrow(() -> validateTimestampRange(null, null));
        Assertions.assertDoesNotThrow(() -> validateTimestampRange(0L, null));
        Assertions.assertDoesNotThrow(() -> validateTimestampRange(null, 1000L));
        Assertions.assertDoesNotThrow(() -> validateTimestampRange(0L, 1L));
        Assertions.assertDoesNotThrow(() -> validateTimestampRange(0L, 1000L));
    }

    @Test
    void testNegativeTimestampFails() {
        assertInvalidTimestampRange(-1L, null);
        assertInvalidTimestampRange(null, -1L);
        assertInvalidTimestampRange(null, 0L);
    }

    @Test
    void testInvalidTimestampRangeFails() {
        // Equal bounds describe an empty half-open range and are rejected before source creation.
        assertInvalidTimestampRange(1000L, 1000L);
        assertInvalidTimestampRange(2000L, 1000L);
    }

    private void assertInvalidTimestampRange(Long startTimestamp, Long endTimestamp) {
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> validateTimestampRange(startTimestamp, endTimestamp));
    }

    private void validateTimestampRange(Long startTimestamp, Long endTimestamp) {
        Map<String, Object> config = new HashMap<>();
        config.put(HbaseBaseOptions.ZOOKEEPER_QUORUM.key(), "127.0.0.1:2181");
        config.put(HbaseBaseOptions.TABLE.key(), "test_table");
        if (startTimestamp != null) {
            config.put(HbaseSourceOptions.START_TIMESTAMP.key(), startTimestamp);
        }
        if (endTimestamp != null) {
            config.put(HbaseSourceOptions.END_TIMESTAMP.key(), endTimestamp);
        }

        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                .validate(new HbaseSourceFactory().optionRule());
    }
}
