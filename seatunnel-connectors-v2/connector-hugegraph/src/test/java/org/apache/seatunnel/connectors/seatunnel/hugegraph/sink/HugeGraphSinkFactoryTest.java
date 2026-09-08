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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkOptions;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HugeGraphSinkFactoryTest {

    private final OptionRule optionRule = new HugeGraphSinkFactory().optionRule();

    /**
     * Every option the sink actually reads must be declared in {@code optionRule()}; otherwise
     * {@code seatunnel.sh --config x --check} (and STATIC dry-run) reject it as an unknown key and
     * it is invisible to option-listing tooling.
     */
    @Test
    void optionRuleDeclaresAllReadOptions() {
        List<Option<?>> optional = optionRule.getOptionalOptions();
        assertTrue(
                optional.contains(HugeGraphSinkOptions.DATA_SAVE_MODE), "data_save_mode missing");
        assertTrue(optional.contains(HugeGraphOptions.CHECK_VERTEX), "check_vertex missing");
        assertTrue(
                optional.contains(HugeGraphOptions.BATCH_FAILURE_FALLBACK),
                "batch_failure_fallback missing");
        assertTrue(
                optional.contains(HugeGraphOptions.MAX_INSERT_ERRORS), "max_insert_errors missing");
        assertTrue(
                optional.contains(HugeGraphOptions.FAILURE_DATA_PATH), "failure_data_path missing");
        assertTrue(
                optional.contains(HugeGraphOptions.RETRY_BACKOFF_MAX_MS),
                "retry_backoff_max_ms missing");
    }

    @Test
    void acceptsValidConnectionOptions() {
        assertDoesNotThrow(() -> validate(requiredConfig()));

        Map<String, Object> config = requiredConfig();
        config.put(HugeGraphOptions.PROTOCOL.key(), "HTTPS");
        config.put(HugeGraphOptions.USERNAME.key(), "user");
        config.put(HugeGraphOptions.PASSWORD.key(), "password");
        config.put(HugeGraphOptions.MAX_RETRIES.key(), 0);
        config.put(HugeGraphOptions.RETRY_BACKOFF_MS.key(), 0);
        config.put(HugeGraphOptions.RETRY_BACKOFF_MAX_MS.key(), 0);

        assertDoesNotThrow(() -> validate(config));
    }

    @Test
    void rejectsBlankRequiredConnectionOptions() {
        Map<String, Object> blankHost = requiredConfig();
        blankHost.put(HugeGraphOptions.HOST.key(), " ");
        assertThrows(OptionValidationException.class, () -> validate(blankHost));

        Map<String, Object> blankGraphName = requiredConfig();
        blankGraphName.put(HugeGraphOptions.GRAPH_NAME.key(), "\t");
        assertThrows(OptionValidationException.class, () -> validate(blankGraphName));
    }

    @Test
    void rejectsPortOutsideValidRange() {
        Map<String, Object> portTooLow = requiredConfig();
        portTooLow.put(HugeGraphOptions.PORT.key(), 0);
        assertThrows(OptionValidationException.class, () -> validate(portTooLow));

        Map<String, Object> portTooHigh = requiredConfig();
        portTooHigh.put(HugeGraphOptions.PORT.key(), 65536);
        assertThrows(OptionValidationException.class, () -> validate(portTooHigh));
    }

    @Test
    void rejectsUnsupportedProtocol() {
        Map<String, Object> config = requiredConfig();
        config.put(HugeGraphOptions.PROTOCOL.key(), "ftp");

        assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    void requiresCredentialsToBeConfiguredTogether() {
        Map<String, Object> usernameOnly = requiredConfig();
        usernameOnly.put(HugeGraphOptions.USERNAME.key(), "user");
        assertThrows(OptionValidationException.class, () -> validate(usernameOnly));

        Map<String, Object> passwordOnly = requiredConfig();
        passwordOnly.put(HugeGraphOptions.PASSWORD.key(), "password");
        assertThrows(OptionValidationException.class, () -> validate(passwordOnly));
    }

    @Test
    void rejectsNegativeRetryOptions() {
        assertNegativeOptionRejected(HugeGraphOptions.MAX_RETRIES);
        assertNegativeOptionRejected(HugeGraphOptions.RETRY_BACKOFF_MS);
        assertNegativeOptionRejected(HugeGraphOptions.RETRY_BACKOFF_MAX_MS);
    }

    private void assertNegativeOptionRejected(Option<Integer> option) {
        Map<String, Object> config = requiredConfig();
        config.put(option.key(), -1);
        assertThrows(OptionValidationException.class, () -> validate(config));
    }

    private void validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "HugeGraphSink");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }

    private static Map<String, Object> requiredConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(HugeGraphOptions.HOST.key(), "127.0.0.1");
        config.put(HugeGraphOptions.PORT.key(), 8080);
        config.put(HugeGraphOptions.GRAPH_NAME.key(), "hugegraph");
        return config;
    }
}
