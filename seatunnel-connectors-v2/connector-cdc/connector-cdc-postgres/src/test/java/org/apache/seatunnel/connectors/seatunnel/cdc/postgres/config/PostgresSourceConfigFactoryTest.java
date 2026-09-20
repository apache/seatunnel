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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresIncrementalSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresSourceOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Locale;

public class PostgresSourceConfigFactoryTest {

    @Test
    public void shouldDeclareStopModeInRuntimeFactoryRule() {
        Assertions.assertTrue(
                new PostgresIncrementalSourceFactory()
                        .optionRule()
                        .getOptionalOptions()
                        .contains(PostgresSourceOptions.STOP_MODE));
    }

    @Test
    public void shouldKeepNeverAsDefaultStopMode() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.emptyMap());

        Assertions.assertEquals(StopMode.NEVER, config.get(PostgresSourceOptions.STOP_MODE));
        Assertions.assertDoesNotThrow(() -> ConfigValidator.of(config).validate(stopModeRule()));
    }

    @Test
    public void shouldAcceptExplicitNeverStopMode() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(Collections.singletonMap("stop.mode", "never"));

        Assertions.assertDoesNotThrow(() -> ConfigValidator.of(config).validate(stopModeRule()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"specific", "latest", "timestamp"})
    public void shouldRejectUnsupportedBoundedStopMode(String mode) {
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.singletonMap("stop.mode", mode));

        OptionValidationException error =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(config).validate(stopModeRule()));
        Assertions.assertTrue(error.getMessage().contains("stop.mode"));
        Assertions.assertTrue(error.getMessage().contains(mode.toUpperCase(Locale.ROOT)));
    }

    private static OptionRule stopModeRule() {
        return OptionRule.builder().optional(PostgresSourceOptions.STOP_MODE).build();
    }

    @Test
    public void shouldDisableDebeziumSnapshotForCommittedOffsetStartup() {
        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname("localhost")
                                .username("user")
                                .password("password")
                                .databaseList("database")
                                .startupOptions(
                                        new StartupConfig(
                                                StartupMode.COMMITTED_OFFSET, null, null, null));

        Assertions.assertEquals(
                "never", configFactory.create(0).getDbzConfiguration().getString("snapshot.mode"));
    }
}
