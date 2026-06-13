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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class JdbcSinkFactoryTest {

    private final OptionRule rule = new JdbcSinkFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> baseConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:oracle:thin:@//localhost:1521/ORCL");
        cfg.put("driver", "oracle.jdbc.OracleDriver");
        cfg.put("schema_save_mode", "CREATE_SCHEMA_WHEN_NOT_EXIST");
        cfg.put("data_save_mode", "APPEND_DATA");
        cfg.put("generate_sink_sql", true);
        cfg.put("database", "ORCL");
        return cfg;
    }

    @Test
    void testValidSinkConfig() {
        Assertions.assertDoesNotThrow(() -> validate(baseConfig()));
    }

    @Test
    void testOracleAppendValuesValidConfig() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("generate_sink_sql", true);
        cfg.put("auto_commit", true);
        cfg.put("is_exactly_once", false);
        cfg.put("use_copy_statement", false);
        cfg.put("support_upsert_by_insert_only", false);
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testExactlyOnceWithMaxRetriesZero() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("is_exactly_once", true);
        cfg.put("max_retries", 0);
        cfg.put("xa_data_source_class_name", "oracle.jdbc.xa.OracleXADataSource");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testOracleAppendValuesWithExactlyOnceFails() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("generate_sink_sql", true);
        cfg.put("is_exactly_once", true);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testOracleAppendValuesWithCopyStatementFails() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("generate_sink_sql", true);
        cfg.put("use_copy_statement", true);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testOracleAppendValuesWithAutoCommitFalseFails() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("generate_sink_sql", true);
        cfg.put("auto_commit", false);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testOracleAppendValuesWithCustomQueryFails() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("generate_sink_sql", false);
        cfg.remove("database");
        cfg.put("query", "INSERT INTO t VALUES(?,?)");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testOracleAppendValuesWithInsertOnlyUpsertFails() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("generate_sink_sql", true);
        cfg.put("support_upsert_by_insert_only", true);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testExactlyOnceWithMaxRetriesNonZeroFails() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("is_exactly_once", true);
        cfg.put("max_retries", 3);
        cfg.put("xa_data_source_class_name", "oracle.jdbc.xa.OracleXADataSource");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingUrlFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("driver", "oracle.jdbc.OracleDriver");
        cfg.put("schema_save_mode", "CREATE_SCHEMA_WHEN_NOT_EXIST");
        cfg.put("data_save_mode", "APPEND_DATA");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingDriverFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:oracle:thin:@//localhost:1521/ORCL");
        cfg.put("schema_save_mode", "CREATE_SCHEMA_WHEN_NOT_EXIST");
        cfg.put("data_save_mode", "APPEND_DATA");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }
}
