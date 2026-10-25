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
package org.apache.seatunnel.connectors.seatunnel.fluss.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.StartMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class FlussSourceFactoryTest {

    private final OptionRule sourceRule = new FlussSourceFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(sourceRule);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(FlussSourceOptions.BOOTSTRAP_SERVERS.key(), "localhost:9123");
        cfg.put(FlussSourceOptions.DATABASE.key(), "fluss_db");
        cfg.put(FlussSourceOptions.TABLE.key(), "fluss_table");
        return cfg;
    }

    @Test
    void testFactoryIdentifierAndSourceClass() {
        FlussSourceFactory factory = new FlussSourceFactory();
        Assertions.assertEquals(FlussSourceOptions.CONNECTOR_IDENTITY, factory.factoryIdentifier());
        Assertions.assertEquals(FlussSource.class, factory.getSourceClass());
    }

    @Test
    void testValidConfig() {
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));
    }

    @Test
    void testValidConfigWithOptionalOptions() {
        Map<String, Object> cfg = validConfig();
        cfg.put(FlussSourceOptions.POLL_TIMEOUT_MS.key(), 5000L);
        Map<String, String> clientConfig = new HashMap<>();
        clientConfig.put("request.timeout", "30s");
        cfg.put(FlussSourceOptions.CLIENT_CONFIG.key(), clientConfig);
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testMissingRequiredOptionRejected() {
        Map<String, Object> cfg = validConfig();
        cfg.remove(FlussSourceOptions.TABLE.key());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testBlankBootstrapServersRejected() {
        Map<String, Object> emptyCfg = validConfig();
        emptyCfg.put(FlussSourceOptions.BOOTSTRAP_SERVERS.key(), "");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(emptyCfg));

        Map<String, Object> whitespaceCfg = validConfig();
        whitespaceCfg.put(FlussSourceOptions.BOOTSTRAP_SERVERS.key(), "   ");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(whitespaceCfg));
    }

    @Test
    void testBlankDatabaseRejected() {
        Map<String, Object> cfg = validConfig();
        cfg.put(FlussSourceOptions.DATABASE.key(), "   ");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testBlankTableRejected() {
        Map<String, Object> cfg = validConfig();
        cfg.put(FlussSourceOptions.TABLE.key(), "");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testStartModeIsOptionalAndDefaultsToEarliest() {
        Assertions.assertEquals(StartMode.EARLIEST, FlussSourceOptions.START_MODE.defaultValue());
        Assertions.assertEquals(
                StartMode.EARLIEST,
                ReadonlyConfig.fromMap(validConfig()).get(FlussSourceOptions.START_MODE));
    }

    @Test
    void testStartModeResolvesCaseInsensitively() {
        Map<String, Object> lower = validConfig();
        lower.put(FlussSourceOptions.START_MODE.key(), "latest");
        Assertions.assertDoesNotThrow(() -> validate(lower));
        Assertions.assertEquals(
                StartMode.LATEST, ReadonlyConfig.fromMap(lower).get(FlussSourceOptions.START_MODE));

        Map<String, Object> upper = validConfig();
        upper.put(FlussSourceOptions.START_MODE.key(), "EARLIEST");
        Assertions.assertEquals(
                StartMode.EARLIEST,
                ReadonlyConfig.fromMap(upper).get(FlussSourceOptions.START_MODE));
    }

    @Test
    void testUnknownStartModeRejected() {
        Map<String, Object> cfg = validConfig();
        cfg.put(FlussSourceOptions.START_MODE.key(), "newest");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> ReadonlyConfig.fromMap(cfg).get(FlussSourceOptions.START_MODE));
    }

    @Test
    void testStartModeLatestRejectedInBatchMode() {
        IllegalArgumentException ex =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> FlussSource.checkStartMode(JobMode.BATCH, StartMode.LATEST));
        Assertions.assertTrue(
                ex.getMessage().contains(FlussSourceOptions.START_MODE.key())
                        && ex.getMessage().contains("BATCH"),
                "Message must name the offending option and job mode: " + ex.getMessage());
    }

    @Test
    void testNonPositivePollTimeoutRejected() {
        Map<String, Object> zeroCfg = validConfig();
        zeroCfg.put(FlussSourceOptions.POLL_TIMEOUT_MS.key(), 0L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(zeroCfg));

        Map<String, Object> negativeCfg = validConfig();
        negativeCfg.put(FlussSourceOptions.POLL_TIMEOUT_MS.key(), -1L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(negativeCfg));
    }
}
