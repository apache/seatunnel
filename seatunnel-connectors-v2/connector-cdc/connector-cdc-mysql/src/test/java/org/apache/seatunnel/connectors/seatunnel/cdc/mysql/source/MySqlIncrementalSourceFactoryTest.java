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

import static org.apache.seatunnel.connectors.cdc.base.option.SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE;
import static org.apache.seatunnel.connectors.cdc.base.option.SourceOptions.STARTUP_SPECIFIC_OFFSET_POS;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlIncrementalSourceOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySqlIncrementalSourceFactoryTest {

    private static final List<String> SPECIFIC_OFFSET_OPTION_KEYS =
            Arrays.asList(
                    STARTUP_SPECIFIC_OFFSET_FILE.key(),
                    STARTUP_SPECIFIC_OFFSET_POS.key(),
                    MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_GTID_SET.key(),
                    MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS.key(),
                    MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_ROWS.key());

    private static OptionRule optionRule() {
        return new MySqlIncrementalSourceFactory().optionRule();
    }

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new MySqlIncrementalSourceFactory()).optionRule());
    }

    @Test
    public void testFileAndPosOnlySpecificOffsetsRemainValid() {
        Map<String, Object> options = basicOptions();
        options.put(MySqlIncrementalSourceOptions.STARTUP_MODE.key(), "SPECIFIC");
        options.put(STARTUP_SPECIFIC_OFFSET_FILE.key(), "mysql-bin.000004");
        options.put(STARTUP_SPECIFIC_OFFSET_POS.key(), 8937L);
        assertValid(options);
    }

    @Test
    public void testOptionalSpecificOffsetMetadataRemainsValid() {
        Map<String, Object> options = basicOptions();
        options.put(MySqlIncrementalSourceOptions.STARTUP_MODE.key(), "SPECIFIC");
        options.put(STARTUP_SPECIFIC_OFFSET_FILE.key(), "mysql-bin.000004");
        options.put(STARTUP_SPECIFIC_OFFSET_POS.key(), 8937L);
        options.put(
                MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_GTID_SET.key(),
                "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5");
        options.put(MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS.key(), 0L);
        options.put(MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_ROWS.key(), 0L);
        assertValid(options);
    }

    @Test
    public void testConfigurationWithoutSpecificOffsetsRemainsValid() {
        assertValid(basicOptions());
    }

    @Test
    public void testExplicitWrongModeRejectsEverySpecificOffset() {
        for (String optionKey : SPECIFIC_OFFSET_OPTION_KEYS) {
            Map<String, Object> options = basicOptions();
            options.put(MySqlIncrementalSourceOptions.STARTUP_MODE.key(), "EARLIEST");
            options.put(optionKey, sampleValue(optionKey));
            OptionValidationException error = assertValidationFails(options);
            Assertions.assertTrue(
                    error.getMessage().contains("specific-offset")
                            && error.getMessage().contains("can only be used when"),
                    () ->
                            "explicit wrong mode must reject '"
                                    + optionKey
                                    + "': "
                                    + error.getMessage());
        }
    }

    @Test
    public void testOmittedModeRejectsEverySpecificOffset() {
        for (String optionKey : SPECIFIC_OFFSET_OPTION_KEYS) {
            Map<String, Object> options = basicOptions();
            options.put(optionKey, sampleValue(optionKey));
            OptionValidationException error = assertValidationFails(options);
            Assertions.assertTrue(
                    error.getMessage().contains("specific-offset")
                            && error.getMessage().contains("can only be used when"),
                    () -> "omitted mode must reject '" + optionKey + "': " + error.getMessage());
        }
    }

    @Test
    public void testPartialAnchorsAreRejected() {
        Map<String, Object> fileOnly = basicOptions();
        fileOnly.put(MySqlIncrementalSourceOptions.STARTUP_MODE.key(), "SPECIFIC");
        fileOnly.put(STARTUP_SPECIFIC_OFFSET_FILE.key(), "mysql-bin.000004");
        OptionValidationException fileOnlyError = assertValidationFails(fileOnly);
        Assertions.assertTrue(
                fileOnlyError.getMessage().contains(STARTUP_SPECIFIC_OFFSET_POS.key()),
                () -> "file-only must complain about missing pos: " + fileOnlyError.getMessage());

        Map<String, Object> posOnly = basicOptions();
        posOnly.put(MySqlIncrementalSourceOptions.STARTUP_MODE.key(), "SPECIFIC");
        posOnly.put(STARTUP_SPECIFIC_OFFSET_POS.key(), 8937L);
        OptionValidationException posOnlyError = assertValidationFails(posOnly);
        Assertions.assertTrue(
                posOnlyError.getMessage().contains(STARTUP_SPECIFIC_OFFSET_FILE.key()),
                () -> "pos-only must complain about missing file: " + posOnlyError.getMessage());
    }

    @Test
    public void testBlankSpecificOffsetValuesAreRejected() {
        Map<String, Object> blankGtid = specificOffsets();
        blankGtid.put(MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_GTID_SET.key(), " ");
        OptionValidationException gtidError = assertValidationFails(blankGtid);
        Assertions.assertTrue(
                gtidError
                                .getMessage()
                                .contains(
                                        MySqlIncrementalSourceOptions
                                                .STARTUP_SPECIFIC_OFFSET_GTID_SET
                                                .key())
                        && gtidError.getMessage().contains("must not be blank"),
                () -> "blank gtid-set must be rejected: " + gtidError.getMessage());

        Map<String, Object> blankFile = specificOffsets();
        blankFile.put(STARTUP_SPECIFIC_OFFSET_FILE.key(), "");
        OptionValidationException fileError = assertValidationFails(blankFile);
        Assertions.assertTrue(
                fileError.getMessage().contains(STARTUP_SPECIFIC_OFFSET_FILE.key())
                        && fileError.getMessage().contains("must not be blank"),
                () -> "blank file must be rejected: " + fileError.getMessage());
    }

    @Test
    public void testNegativeSkipValuesAreRejected() {
        for (String optionKey :
                Arrays.asList(
                        MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS.key(),
                        MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_ROWS.key())) {
            Map<String, Object> options = specificOffsets();
            options.put(optionKey, -1L);
            OptionValidationException error = assertValidationFails(options);
            Assertions.assertTrue(
                    error.getMessage().contains(optionKey)
                            && error.getMessage().contains("must be greater than or equal to 0"),
                    () -> "negative skip must be rejected: " + error.getMessage());
        }
    }

    private static Map<String, Object> basicOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put(MySqlIncrementalSourceOptions.USERNAME.key(), "root");
        options.put(MySqlIncrementalSourceOptions.PASSWORD.key(), "password");
        options.put(MySqlIncrementalSourceOptions.URL.key(), "jdbc:mysql://localhost:3306");
        options.put(MySqlIncrementalSourceOptions.TABLE_PATTERN.key(), "db\\..*");
        return options;
    }

    private static Map<String, Object> specificOffsets() {
        Map<String, Object> options = basicOptions();
        options.put(MySqlIncrementalSourceOptions.STARTUP_MODE.key(), "SPECIFIC");
        options.put(STARTUP_SPECIFIC_OFFSET_FILE.key(), "mysql-bin.000004");
        options.put(STARTUP_SPECIFIC_OFFSET_POS.key(), 8937L);
        return options;
    }

    private static Object sampleValue(String optionKey) {
        if (MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS
                        .key()
                        .equals(optionKey)
                || MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_ROWS
                        .key()
                        .equals(optionKey)) {
            return 0L;
        }
        if (STARTUP_SPECIFIC_OFFSET_POS.key().equals(optionKey)) {
            return 8937L;
        }
        return "mysql-bin.000004";
    }

    private static void assertValid(Map<String, Object> options) {
        ConfigValidator.of(ReadonlyConfig.fromMap(options)).validate(optionRule());
    }

    private static OptionValidationException assertValidationFails(Map<String, Object> options) {
        return Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(options)).validate(optionRule()));
    }
}
