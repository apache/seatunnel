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

package org.apache.seatunnel.connectors.seatunnel.redshift.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3FileBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class S3RedshiftSinkFactoryTest {

    private final OptionRule optionRule = new S3RedshiftSinkFactory().optionRule();

    @Test
    void testValidConfiguration() {
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));
    }

    @Test
    void testBlankRedshiftOptionsRejected() {
        for (String key :
                new String[] {
                    S3RedshiftSinkOptions.JDBC_URL.key(),
                    S3RedshiftSinkOptions.JDBC_USER.key(),
                    S3RedshiftSinkOptions.JDBC_PASSWORD.key(),
                    S3RedshiftSinkOptions.EXECUTE_SQL.key()
                }) {
            assertRejected(key, "");
            assertRejected(key, "   \t");
        }
    }

    private void assertRejected(String key, String value) {
        Map<String, Object> config = validConfig();
        config.put(key, value);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config), key);
    }

    private void validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "S3Redshift");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(S3FileBaseOptions.S3_BUCKET.key(), "bucket");
        config.put(S3RedshiftSinkOptions.JDBC_URL.key(), "jdbc:redshift://localhost:5439/dev");
        config.put(S3RedshiftSinkOptions.JDBC_USER.key(), "test-user");
        config.put(S3RedshiftSinkOptions.JDBC_PASSWORD.key(), "test-password");
        config.put(S3RedshiftSinkOptions.EXECUTE_SQL.key(), "select 1");
        config.put(FileBaseSourceOptions.FILE_PATH.key(), "/path");
        config.put(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key(), "test-provider");
        return config;
    }
}
