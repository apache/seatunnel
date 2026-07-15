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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

class ConfigValidationUtilsTest {

    @Test
    void shouldValidateHappyPathConfig() throws URISyntaxException {
        Config config =
                ConfigBuilder.of(getResourcePath("/config/validation_fake_to_inmemory.json"));
        Assertions.assertDoesNotThrow(() -> ConfigValidationUtils.validate(config));
    }

    @Test
    void shouldFailWhenSinkInputTableIsMissing() throws URISyntaxException {
        Config config =
                ConfigBuilder.of(getResourcePath("/config/validation_missing_input_table.json"));

        ConfigCheckException exception =
                Assertions.assertThrows(
                        ConfigCheckException.class, () -> ConfigValidationUtils.validate(config));

        Assertions.assertEquals("table missing_table not found", exception.getMessage());
    }

    @Test
    void shouldFailWhenSinkHasMultipleInputTables() throws URISyntaxException {
        Config config =
                ConfigBuilder.of(getResourcePath("/config/validation_multiple_input_tables.json"));

        ConfigCheckException exception =
                Assertions.assertThrows(
                        ConfigCheckException.class, () -> ConfigValidationUtils.validate(config));

        Assertions.assertEquals(
                "Multiple input tables are not supported in the current version",
                exception.getMessage());
    }

    @Test
    void shouldFailWhenSinkFactoryRejectsConfig() throws URISyntaxException {
        Config config =
                ConfigBuilder.of(getResourcePath("/config/validation_sink_assert_failure.json"));

        ConfigCheckException exception =
                Assertions.assertThrows(
                        ConfigCheckException.class, () -> ConfigValidationUtils.validate(config));

        Assertions.assertTrue(exception.getMessage().contains("assert key and value not match"));
    }

    @Test
    void shouldFailWhenSourceSectionIsMissing() throws URISyntaxException {
        Config config =
                ConfigBuilder.of(getResourcePath("/config/validation_fake_to_inmemory.json"))
                        .withoutPath(Constants.SOURCE);

        ConfigCheckException exception =
                Assertions.assertThrows(
                        ConfigCheckException.class, () -> ConfigValidationUtils.validate(config));

        Assertions.assertEquals(
                "At least one source plugin must be configured.", exception.getMessage());
    }

    @Test
    void shouldFailWhenSinkSectionIsMissing() throws URISyntaxException {
        Config config =
                ConfigBuilder.of(getResourcePath("/config/validation_fake_to_inmemory.json"))
                        .withoutPath(Constants.SINK);

        ConfigCheckException exception =
                Assertions.assertThrows(
                        ConfigCheckException.class, () -> ConfigValidationUtils.validate(config));

        Assertions.assertEquals(
                "At least one sink plugin must be configured.", exception.getMessage());
    }

    private static Path getResourcePath(String resourcePath) throws URISyntaxException {
        URL resource = ConfigValidationUtilsTest.class.getResource(resourcePath);
        Assertions.assertNotNull(resource);
        return Paths.get(resource.toURI());
    }
}
