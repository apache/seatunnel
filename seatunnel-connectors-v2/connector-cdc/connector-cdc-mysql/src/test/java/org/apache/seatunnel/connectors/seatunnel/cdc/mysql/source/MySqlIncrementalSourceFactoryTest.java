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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MySqlIncrementalSourceFactoryTest {
    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new MySqlIncrementalSourceFactory()).optionRule());
    }

    @Test
    public void testSupportedStartUpModes() {
        new MySqlIncrementalSourceFactory()
                .optionRule().getOptionalOptions().stream()
                        .filter(option -> option.key().equals(SourceOptions.STARTUP_MODE_KEY))
                        .forEach(
                                option ->
                                        Assertions.assertIterableEquals(
                                                Arrays.asList(
                                                        StartupMode.INITIAL,
                                                        StartupMode.SNAPSHOT,
                                                        StartupMode.EARLIEST,
                                                        StartupMode.LATEST,
                                                        StartupMode.SPECIFIC,
                                                        StartupMode.TIMESTAMP),
                                                ((SingleChoiceOption<StartupMode>) option)
                                                        .getOptionValues()));
    }

    @Test
    public void testSnapshotModeRejectsStopOptions() {
        // snapshot mode owns its bounded stop boundary, so explicit stop.* options must be
        // rejected at validation time rather than silently ignored at runtime.
        Map<String, Object> options = new HashMap<>();
        options.put(SourceOptions.STARTUP_MODE_KEY, "snapshot");
        options.put(SourceOptions.STOP_TIMESTAMP.key(), "1000");
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new MySqlIncrementalSource<>(
                                        config, Collections.<CatalogTable>emptyList()));
        Assertions.assertTrue(exception.getMessage().contains("stop offset options"));
    }
}
