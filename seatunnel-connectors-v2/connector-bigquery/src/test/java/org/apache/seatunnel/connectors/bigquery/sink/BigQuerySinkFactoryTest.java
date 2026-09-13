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

package org.apache.seatunnel.connectors.bigquery.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BigQuerySinkFactoryTest {

    @Test
    void testStreamingModeDoesNotRequireSequenceNumberColumn() {
        Config config =
                ConfigFactory.parseString(
                        requiredOptions()
                                + BigQuerySinkOptions.WRITE_MODE.key()
                                + " = \"streaming\"\n"
                                + BigQuerySinkOptions.SERVICE_ACCOUNT_KEY_JSON.key()
                                + " = \"{}\"\n");

        BigQuerySinkFactory factory = new BigQuerySinkFactory();

        assertDoesNotThrow(
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromConfig(config))
                                .validate(factory.optionRule()));
    }

    @Test
    void testUniverseDomainConfigurationParsing() {
        Config config =
                ConfigFactory.parseString(
                        requiredOptions()
                                + BigQuerySinkOptions.UNIVERSE_DOMAIN.key()
                                + " = \"s3nsapis.fr\"\n");

        BigQuerySinkFactory factory = new BigQuerySinkFactory();
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);

        assertDoesNotThrow(() -> ConfigValidator.of(readonlyConfig).validate(factory.optionRule()));

        org.junit.jupiter.api.Assertions.assertEquals(
                "s3nsapis.fr", readonlyConfig.get(BigQuerySinkOptions.UNIVERSE_DOMAIN));
    }

    @Test
    void testInvalidWriteModeRejectedByOptionRule() {
        Config config =
                ConfigFactory.parseString(
                        requiredOptions()
                                + BigQuerySinkOptions.WRITE_MODE.key()
                                + " = \"invalid\"\n");

        BigQuerySinkFactory factory = new BigQuerySinkFactory();

        assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromConfig(config))
                                .validate(factory.optionRule()));
    }

    @Test
    void testInvalidWriteModeRejectedBySinkConstructor() {
        Config config =
                ConfigFactory.parseString(
                        requiredOptions()
                                + BigQuerySinkOptions.WRITE_MODE.key()
                                + " = \"invalid\"\n");

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);

        assertThrows(SeaTunnelRuntimeException.class, () -> new BigQuerySink(readonlyConfig, null));
    }

    @Test
    void testNonPositiveBatchSizeRejectedByOptionRule() {
        Config config =
                ConfigFactory.parseString(
                        requiredOptions() + BigQuerySinkOptions.BATCH_SIZE.key() + " = 0\n");

        BigQuerySinkFactory factory = new BigQuerySinkFactory();

        assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromConfig(config))
                                .validate(factory.optionRule()));
    }

    @Test
    void testBlankRequiredOptionRejectedByOptionRule() {
        Config config =
                ConfigFactory.parseString(
                        BigQuerySinkOptions.PROJECT_ID.key()
                                + " = \" \"\n"
                                + BigQuerySinkOptions.DATASET_ID.key()
                                + " = \"test_dataset\"\n"
                                + BigQuerySinkOptions.TABLE_ID.key()
                                + " = \"test_table\"\n");

        BigQuerySinkFactory factory = new BigQuerySinkFactory();

        assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromConfig(config))
                                .validate(factory.optionRule()));
    }

    private static String requiredOptions() {
        return BigQuerySinkOptions.PROJECT_ID.key()
                + " = \"test-project\"\n"
                + BigQuerySinkOptions.DATASET_ID.key()
                + " = \"test_dataset\"\n"
                + BigQuerySinkOptions.TABLE_ID.key()
                + " = \"test_table\"\n";
    }
}
