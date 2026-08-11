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

package org.apache.seatunnel.connectors.doris.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DorisSinkConfigTest {

    @Test
    void testDirectToBeRequiresBenodes() {
        ReadonlyConfig config = createConfig(Collections.singletonMap("direct_to_be", true));

        DorisConnectorException exception =
                Assertions.assertThrows(
                        DorisConnectorException.class, () -> DorisSinkConfig.of(config));

        Assertions.assertTrue(exception.getMessage().contains("direct_to_be"));
        Assertions.assertTrue(exception.getMessage().contains("benodes"));
    }

    @Test
    void testDirectToBeRejectsBlankBenodes() {
        ReadonlyConfig config =
                createConfig(
                        new HashMap<String, Object>() {
                            {
                                put("direct_to_be", true);
                                put("benodes", "   ");
                            }
                        });

        DorisConnectorException exception =
                Assertions.assertThrows(
                        DorisConnectorException.class, () -> DorisSinkConfig.of(config));

        Assertions.assertTrue(exception.getMessage().contains("direct_to_be"));
        Assertions.assertTrue(exception.getMessage().contains("benodes"));
    }

    @Test
    void testBenodesRemainInactiveWhenDirectToBeDisabled() {
        ReadonlyConfig config =
                createConfig(
                        new HashMap<String, Object>() {
                            {
                                put("direct_to_be", false);
                                put("benodes", "be1:8040,be2:8040");
                            }
                        });

        DorisSinkConfig sinkConfig = DorisSinkConfig.of(config);

        Assertions.assertEquals("be1:8040,be2:8040", sinkConfig.getBackends());
        Assertions.assertFalse(sinkConfig.isDirectToBe());
    }

    @Test
    void testDatetimeTimezoneDefaultsToNull() {
        DorisSinkConfig sinkConfig = DorisSinkConfig.of(createConfig(Collections.emptyMap()));
        Assertions.assertNull(sinkConfig.getDatetimeTimezone());
    }

    @Test
    void testDatetimeTimezoneAcceptsRegionId() {
        ReadonlyConfig config =
                createConfig(Collections.singletonMap("sink.datetime-timezone", "Asia/Shanghai"));
        DorisSinkConfig sinkConfig = DorisSinkConfig.of(config);
        Assertions.assertEquals(ZoneId.of("Asia/Shanghai"), sinkConfig.getDatetimeTimezone());
    }

    @Test
    void testDatetimeTimezoneAcceptsOffsetId() {
        ReadonlyConfig config =
                createConfig(Collections.singletonMap("sink.datetime-timezone", "+08:00"));
        DorisSinkConfig sinkConfig = DorisSinkConfig.of(config);
        Assertions.assertEquals(ZoneId.of("+08:00"), sinkConfig.getDatetimeTimezone());
    }

    @Test
    void testDatetimeTimezoneRejectsInvalidId() {
        ReadonlyConfig config =
                createConfig(Collections.singletonMap("sink.datetime-timezone", "Not/A_Zone"));
        DorisConnectorException exception =
                Assertions.assertThrows(
                        DorisConnectorException.class, () -> DorisSinkConfig.of(config));
        Assertions.assertTrue(exception.getMessage().contains("sink.datetime-timezone"));
    }

    private ReadonlyConfig createConfig(Map<String, Object> extraOptions) {
        Map<String, Object> options = new HashMap<>();
        options.put("fenodes", "fe1:8030");
        options.put("username", "root");
        options.put("password", "");
        options.put("database", "test_db");
        options.put("table", "test_table");
        options.put("sink.label-prefix", "test_job");
        options.put("doris.config", Collections.singletonMap("format", "json"));
        options.putAll(extraOptions);
        return ReadonlyConfig.fromMap(options);
    }
}
