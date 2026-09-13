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

package org.apache.seatunnel.connectors.seatunnel.openmldb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.openmldb.source.OpenMldbSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

class OpenMldbFactoryTest {

    private final OptionRule optionRule = new OpenMldbSourceFactory().optionRule();

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNonblankSqlAccepted(boolean clusterMode) {
        for (String sql :
                new String[] {
                    "select * from test_table", " select * from test_table ", "not parsed here"
                }) {
            Map<String, Object> config = requiredConfig(clusterMode);
            config.put("sql", sql);
            Assertions.assertDoesNotThrow(() -> validate(config));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMissingSqlRejected(boolean clusterMode) {
        Map<String, Object> config = requiredConfig(clusterMode);
        config.remove("sql");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEmptySqlRejected(boolean clusterMode) {
        assertInvalidSql(clusterMode, "");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testWhitespaceOnlySqlRejected(boolean clusterMode) {
        assertInvalidSql(clusterMode, " ");
        assertInvalidSql(clusterMode, "\t\r\n");
    }

    private void assertInvalidSql(boolean clusterMode, String sql) {
        Map<String, Object> config = requiredConfig(clusterMode);
        config.put("sql", sql);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    private void validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "OpenMldb");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }

    private Map<String, Object> requiredConfig(boolean clusterMode) {
        Map<String, Object> config = new HashMap<>();
        config.put("cluster_mode", clusterMode);
        config.put("database", "test_db");
        config.put("sql", "select * from test_table");
        if (clusterMode) {
            config.put("zk_host", "localhost:2181");
            config.put("zk_path", "/openmldb");
        } else {
            config.put("host", "localhost");
            config.put("port", 6527);
        }
        return config;
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull((new OpenMldbSourceFactory()).optionRule());
    }
}
