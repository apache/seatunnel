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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Tests validation and normalization of GaussDB mppdb connector options. */
class GaussDBMppdbConfigTest {

    /** Verifies defaults select serial mppdb decoding. */
    @Test
    void testMppdbDefaults() {
        GaussDBMppdbConfig config = new GaussDBMppdbConfig(config(new HashMap<>()));

        Assertions.assertTrue(config.usesMppdbDecoding());
        Assertions.assertEquals("seatunnel", config.getSlotName());
        Assertions.assertEquals(1, config.getParallelDecodeNum());
        Assertions.assertEquals("b", config.getDecodeStyle());
        Assertions.assertFalse(config.isSendingBatch());
    }

    /** Verifies dedicated replication and parallel decoder options. */
    @Test
    void testMppdbParallelOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put(GaussDBIncrementalSourceOptions.REPLICATION_PORT.key(), 5433);
        options.put(GaussDBIncrementalSourceOptions.PARALLEL_DECODE_NUM.key(), 4);
        options.put(GaussDBIncrementalSourceOptions.DECODE_STYLE.key(), "J");
        options.put(GaussDBIncrementalSourceOptions.SENDING_BATCH.key(), true);

        GaussDBMppdbConfig config = new GaussDBMppdbConfig(config(options));

        Assertions.assertEquals(5433, config.getReplicationPort());
        Assertions.assertEquals(4, config.getParallelDecodeNum());
        Assertions.assertEquals("j", config.getDecodeStyle());
        Assertions.assertTrue(config.isSendingBatch());
    }

    /** Verifies invalid parallelism and decode styles are rejected. */
    @Test
    void testRejectInvalidMppdbOptions() {
        Map<String, Object> invalidParallelism = new HashMap<>();
        invalidParallelism.put(GaussDBIncrementalSourceOptions.PARALLEL_DECODE_NUM.key(), 21);
        Map<String, Object> invalidStyle = new HashMap<>();
        invalidStyle.put(GaussDBIncrementalSourceOptions.DECODE_STYLE.key(), "x");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new GaussDBMppdbConfig(config(invalidParallelism)));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> new GaussDBMppdbConfig(config(invalidStyle)));
    }

    /** Verifies mppdb-only constraints do not affect standard PostgreSQL plugins. */
    @Test
    void testIgnoreMppdbOnlyOptionsForPostgresPlugin() {
        Map<String, Object> options = new HashMap<>();
        options.put(GaussDBIncrementalSourceOptions.DECODING_PLUGIN_NAME.key(), "pgoutput");
        options.put(GaussDBIncrementalSourceOptions.PARALLEL_DECODE_NUM.key(), 99);
        options.put(GaussDBIncrementalSourceOptions.DECODE_STYLE.key(), "unused");

        GaussDBMppdbConfig config = new GaussDBMppdbConfig(config(options));

        Assertions.assertFalse(config.usesMppdbDecoding());
    }

    /** Creates a minimal source configuration with supplied overrides. */
    private ReadonlyConfig config(Map<String, Object> options) {
        options.put(JdbcCommonOptions.URL.key(), "jdbc:postgresql://localhost:5432/gaussdb");
        return ReadonlyConfig.fromMap(options);
    }
}
