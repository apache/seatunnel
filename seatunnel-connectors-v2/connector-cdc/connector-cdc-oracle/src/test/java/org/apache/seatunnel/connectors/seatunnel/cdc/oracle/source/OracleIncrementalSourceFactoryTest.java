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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.offset.RedoLogOffset;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class OracleIncrementalSourceFactoryTest {
    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new OracleIncrementalSourceFactory()).optionRule());
    }

    @Test
    public void testOnlyNeverStopModeIsSupported() {
        new OracleIncrementalSourceFactory()
                .optionRule().getOptionalOptions().stream()
                        .filter((option) -> option.key().equals(SourceOptions.STOP_MODE_KEY))
                        .forEach(
                                (option) ->
                                        Assertions.assertIterableEquals(
                                                Collections.singletonList(StopMode.NEVER),
                                                ((SingleChoiceOption<StopMode>) option)
                                                        .getOptionValues()));
    }

    @Test
    public void testSpecificStartupModeRequiresScn() {
        Map<String, Object> config = baseConfig();
        config.put(OracleIncrementalSourceOptions.STARTUP_MODE.key(), StartupMode.SPECIFIC);

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(new OracleIncrementalSourceFactory().optionRule()));
    }

    @Test
    public void testSpecificStartupModeRejectsInvalidScn() {
        Map<String, Object> config = specificStartupConfig(0L);

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(new OracleIncrementalSourceFactory().optionRule()));
    }

    @Test
    public void testSpecificStartupModeUsesScnOffset() {
        StartupConfig startupConfig =
                OracleIncrementalSource.getOracleStartupConfig(
                        ReadonlyConfig.fromMap(specificStartupConfig(123456789L)));

        RedoLogOffset startupOffset =
                (RedoLogOffset) startupConfig.getStartupOffset(new TestOffsetFactory());
        IncrementalSplit split =
                new IncrementalSplit(
                        "oracle-incremental-split",
                        Collections.emptyList(),
                        startupOffset,
                        RedoLogOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList());
        RedoLogOffset restoredOffset =
                (RedoLogOffset) new IncrementalSplitState(split).toSourceSplit().getStartupOffset();

        Assertions.assertEquals(StartupMode.SPECIFIC, startupConfig.getStartupMode());
        Assertions.assertEquals("123456789", startupOffset.getScn());
        Assertions.assertEquals("0", startupOffset.getCommitScn());
        Assertions.assertNull(startupOffset.getLcrPosition());
        Assertions.assertEquals(startupOffset, restoredOffset);
    }

    @Test
    public void testScnOffsetOnlySupportsSpecificStartupMode() {
        Map<String, Object> config = baseConfig();
        config.put(OracleIncrementalSourceOptions.STARTUP_MODE.key(), StartupMode.LATEST);
        config.put(OracleIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SCN.key(), 123456789L);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        OracleIncrementalSource.getOracleStartupConfig(
                                ReadonlyConfig.fromMap(config)));
    }

    @Test
    public void testOracleSpecificStartupModeRejectsFilePositionOffset() {
        Map<String, Object> config = specificStartupConfig(123456789L);
        config.put(SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE.key(), "redo.log");
        config.put(SourceOptions.STARTUP_SPECIFIC_OFFSET_POS.key(), 100L);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        OracleIncrementalSource.getOracleStartupConfig(
                                ReadonlyConfig.fromMap(config)));
    }

    @Test
    public void rejectsEnablingLobOptionsWhenLobMiningIsOff() {
        Map<String, Object> reselect = baseConfig();
        reselect.put(OracleIncrementalSourceOptions.LOB_RESELECT_ENABLED.key(), true);
        OptionValidationException reselectError =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> OracleLobOptionValidator.validate(ReadonlyConfig.fromMap(reselect)));
        Assertions.assertTrue(reselectError.getMessage().contains("lob.reselect.enabled=true"));
        Assertions.assertTrue(reselectError.getMessage().contains("lob.enabled"));
        Assertions.assertFalse(reselectError.getMessage().contains("debezium.lob.enabled ="));

        Map<String, Object> nullHandling = baseConfig();
        nullHandling.put(
                OracleIncrementalSourceOptions.LOB_UNAVAILABLE_VALUE_HANDLING.key(),
                OracleLobUnavailableValueHandling.NULL);
        OptionValidationException nullError =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () ->
                                OracleLobOptionValidator.validate(
                                        ReadonlyConfig.fromMap(nullHandling)));
        Assertions.assertTrue(
                nullError.getMessage().contains("lob.unavailable-value.handling=null"));

        Map<String, Object> failHandling = baseConfig();
        failHandling.put(
                OracleIncrementalSourceOptions.LOB_UNAVAILABLE_VALUE_HANDLING.key(), "fail");
        Map<String, String> disabled = new HashMap<String, String>();
        disabled.put("lob.enabled", "false");
        failHandling.put(SourceOptions.DEBEZIUM_PROPERTIES.key(), disabled);
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> OracleLobOptionValidator.validate(ReadonlyConfig.fromMap(failHandling)));
    }

    @Test
    public void acceptsDefaultsAndNoOpLobOptionsWithoutLobMining() {
        OracleLobOptionValidator.validate(ReadonlyConfig.fromMap(baseConfig()));
        Assertions.assertFalse(
                OracleLobOptionValidator.placeholderHandlingActive(
                        ReadonlyConfig.fromMap(baseConfig())));

        Map<String, Object> explicitNoOp = baseConfig();
        explicitNoOp.put(OracleIncrementalSourceOptions.LOB_RESELECT_ENABLED.key(), false);
        explicitNoOp.put(
                OracleIncrementalSourceOptions.LOB_UNAVAILABLE_VALUE_HANDLING.key(),
                "warn_and_keep");
        ReadonlyConfig config = ReadonlyConfig.fromMap(explicitNoOp);
        OracleLobOptionValidator.validate(config);
        Assertions.assertFalse(OracleLobOptionValidator.placeholderHandlingActive(config));
    }

    @Test
    public void lobMiningUsesReselectAndHandlingDefaultsUntilEnabled() {
        Map<String, String> debezium = new HashMap<String, String>();
        debezium.put("lob.enabled", " true ");
        Map<String, Object> defaults = baseConfig();
        defaults.put(SourceOptions.DEBEZIUM_PROPERTIES.key(), debezium);
        ReadonlyConfig defaultConfig = ReadonlyConfig.fromMap(defaults);
        OracleLobOptionValidator.validate(defaultConfig);
        Assertions.assertFalse(OracleLobOptionValidator.placeholderHandlingActive(defaultConfig));

        Map<String, Object> enabled = baseConfig();
        enabled.put(SourceOptions.DEBEZIUM_PROPERTIES.key(), debezium);
        enabled.put(OracleIncrementalSourceOptions.LOB_RESELECT_ENABLED.key(), true);
        enabled.put(
                OracleIncrementalSourceOptions.LOB_UNAVAILABLE_VALUE_HANDLING.key(),
                OracleLobUnavailableValueHandling.NULL);
        ReadonlyConfig enabledConfig = ReadonlyConfig.fromMap(enabled);
        OracleLobOptionValidator.validate(enabledConfig);
        Assertions.assertTrue(OracleLobOptionValidator.placeholderHandlingActive(enabledConfig));
    }

    @Test
    public void seatunnelLobEnabledPassesThroughAndOverridesDebezium() {
        Map<String, Object> enabled = captureConfig();
        enabled.put(OracleIncrementalSourceOptions.LOB_ENABLED.key(), true);
        enabled.put(OracleIncrementalSourceOptions.LOB_RESELECT_ENABLED.key(), true);
        ReadonlyConfig enabledConfig = ReadonlyConfig.fromMap(enabled);
        OracleLobOptionValidator.validate(enabledConfig);
        Assertions.assertTrue(OracleLobOptionValidator.placeholderHandlingActive(enabledConfig));
        Assertions.assertEquals("true", debeziumLobEnabled(enabledConfig));

        Map<String, String> debezium = new HashMap<String, String>();
        debezium.put("lob.enabled", "true");
        Map<String, Object> overridden = captureConfig();
        overridden.put(SourceOptions.DEBEZIUM_PROPERTIES.key(), debezium);
        overridden.put(OracleIncrementalSourceOptions.LOB_ENABLED.key(), false);
        overridden.put(OracleIncrementalSourceOptions.LOB_RESELECT_ENABLED.key(), true);
        ReadonlyConfig overriddenConfig = ReadonlyConfig.fromMap(overridden);
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> OracleLobOptionValidator.validate(overriddenConfig));
        Assertions.assertEquals("false", debeziumLobEnabled(overriddenConfig));

        Map<String, Object> legacy = captureConfig();
        legacy.put(SourceOptions.DEBEZIUM_PROPERTIES.key(), debezium);
        ReadonlyConfig legacyConfig = ReadonlyConfig.fromMap(legacy);
        OracleLobOptionValidator.validate(legacyConfig);
        Assertions.assertEquals("true", debeziumLobEnabled(legacyConfig));
    }

    private static String debeziumLobEnabled(ReadonlyConfig config) {
        OracleSourceConfigFactory configFactory = new OracleSourceConfigFactory();
        configFactory.fromReadonlyConfig(config);
        configFactory.originUrl(config.get(JdbcCommonOptions.URL));
        OracleIncrementalSource.applyLobEnabled(config, configFactory);
        OracleSourceConfig sourceConfig = configFactory.create(0);
        return sourceConfig.getOriginDbzConnectorConfig().getString("lob.enabled");
    }

    private static Map<String, Object> specificStartupConfig(long scn) {
        Map<String, Object> config = baseConfig();
        config.put(OracleIncrementalSourceOptions.STARTUP_MODE.key(), StartupMode.SPECIFIC);
        config.put(OracleIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SCN.key(), scn);
        return config;
    }

    private static Map<String, Object> baseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(OracleIncrementalSourceOptions.USERNAME.key(), "user");
        config.put(OracleIncrementalSourceOptions.PASSWORD.key(), "password");
        config.put(ConnectorCommonOptions.TABLE_NAMES.key(), Arrays.asList("ORCL.TEST"));
        return config;
    }

    private static Map<String, Object> captureConfig() {
        Map<String, Object> config = baseConfig();
        config.put(JdbcCommonOptions.URL.key(), "jdbc:oracle:thin:@//localhost:1521/ORCLCDB");
        config.put(
                OracleIncrementalSourceOptions.DATABASE_NAMES.key(),
                Collections.singletonList("ORCLCDB"));
        config.put(
                OracleIncrementalSourceOptions.SCHEMA_NAMES.key(),
                Collections.singletonList("DEBEZIUM"));
        return config;
    }

    private static class TestOffsetFactory extends OffsetFactory {

        @Override
        public Offset earliest() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Offset neverStop() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Offset latest() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Offset specific(Map<String, String> offset) {
            return new RedoLogOffset(offset);
        }

        @Override
        public Offset specific(String filename, Long position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Offset timestamp(long timestamp) {
            throw new UnsupportedOperationException();
        }
    }
}
