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
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.offset.RedoLogOffset;

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
