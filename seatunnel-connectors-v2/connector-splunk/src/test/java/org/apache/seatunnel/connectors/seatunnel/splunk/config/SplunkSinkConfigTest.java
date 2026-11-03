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

package org.apache.seatunnel.connectors.seatunnel.splunk.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.splunk.exception.SplunkConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.splunk.exception.SplunkConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class SplunkSinkConfigTest {

    private static Map<String, Object> validOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("url", "https://splunk-host:8088");
        options.put("token", "00000000-0000-0000-0000-000000000000");
        return options;
    }

    private static SplunkSinkConfig configOf(Map<String, Object> options) {
        return new SplunkSinkConfig(ReadonlyConfig.fromMap(options));
    }

    @Test
    void defaultsAreApplied() {
        SplunkSinkConfig config = configOf(validOptions());

        Assertions.assertEquals(
                "https://splunk-host:8088/services/collector/event", config.getEndpoint());
        Assertions.assertEquals("00000000-0000-0000-0000-000000000000", config.getToken());
        Assertions.assertEquals(100, config.getMaxBatchSize());
        Assertions.assertEquals(3, config.getMaxRetryCount());
        Assertions.assertEquals(200, config.getRetryBackoffMs());
        Assertions.assertTrue(config.isTlsVerifyCertificate());
        Assertions.assertTrue(config.isTlsVerifyHostname());
        Assertions.assertNull(config.getIndex());
        Assertions.assertNull(config.getSourceType());
    }

    @Test
    void collectorPathIsAppendedToBaseUrlOnlyOnce() {
        Map<String, Object> options = validOptions();
        options.put("url", "https://splunk-host:8088/services/collector/event");
        Assertions.assertEquals(
                "https://splunk-host:8088/services/collector/event",
                configOf(options).getEndpoint());

        options.put("url", "https://splunk-host:8088/services/collector");
        Assertions.assertEquals(
                "https://splunk-host:8088/services/collector", configOf(options).getEndpoint());
    }

    @Test
    void trailingSlashesAreStripped() {
        Map<String, Object> options = validOptions();
        options.put("url", "https://splunk-host:8088/");
        Assertions.assertEquals(
                "https://splunk-host:8088/services/collector/event",
                configOf(options).getEndpoint());
    }

    @Test
    void missingUrlFailsWithActionableMessage() {
        Map<String, Object> options = validOptions();
        options.remove("url");

        SplunkConnectorException exception =
                Assertions.assertThrows(SplunkConnectorException.class, () -> configOf(options));
        Assertions.assertEquals(
                SplunkConnectorErrorCode.INVALID_CONFIG.getCode(),
                exception.getSeaTunnelErrorCode().getCode());
        Assertions.assertTrue(
                exception.getMessage().contains("'url' is required"), exception.getMessage());
    }

    @Test
    void blankUrlFailsWithActionableMessage() {
        Map<String, Object> options = validOptions();
        options.put("url", "   ");

        SplunkConnectorException exception =
                Assertions.assertThrows(SplunkConnectorException.class, () -> configOf(options));
        Assertions.assertTrue(
                exception.getMessage().contains("'url' is required"), exception.getMessage());
    }

    @Test
    void relativeUrlFailsWithActionableMessage() {
        Map<String, Object> options = validOptions();
        options.put("url", "splunk-host:8088");

        SplunkConnectorException exception =
                Assertions.assertThrows(SplunkConnectorException.class, () -> configOf(options));
        Assertions.assertTrue(
                exception.getMessage().contains("must be an absolute http or https URL"),
                exception.getMessage());
    }

    @Test
    void nonHttpSchemeFailsWithActionableMessage() {
        Map<String, Object> options = validOptions();
        options.put("url", "ftp://splunk-host:8088");

        SplunkConnectorException exception =
                Assertions.assertThrows(SplunkConnectorException.class, () -> configOf(options));
        Assertions.assertTrue(
                exception.getMessage().contains("must be an absolute http or https URL"),
                exception.getMessage());
    }

    @Test
    void missingTokenFailsWithActionableMessage() {
        Map<String, Object> options = validOptions();
        options.remove("token");

        SplunkConnectorException exception =
                Assertions.assertThrows(SplunkConnectorException.class, () -> configOf(options));
        Assertions.assertEquals(
                SplunkConnectorErrorCode.INVALID_CONFIG.getCode(),
                exception.getSeaTunnelErrorCode().getCode());
        Assertions.assertTrue(
                exception.getMessage().contains("'token' is required"), exception.getMessage());
    }

    @Test
    void blankTokenFailsWithActionableMessage() {
        Map<String, Object> options = validOptions();
        options.put("token", " ");

        SplunkConnectorException exception =
                Assertions.assertThrows(SplunkConnectorException.class, () -> configOf(options));
        Assertions.assertTrue(
                exception.getMessage().contains("'token' is required"), exception.getMessage());
    }

    @Test
    void nonPositiveBatchSizeFails() {
        Map<String, Object> options = validOptions();
        options.put("max_batch_size", 0);

        SplunkConnectorException exception =
                Assertions.assertThrows(SplunkConnectorException.class, () -> configOf(options));
        Assertions.assertTrue(
                exception.getMessage().contains("'max_batch_size' must be greater than 0"),
                exception.getMessage());
    }

    @Test
    void negativeRetryBackoffFails() {
        Map<String, Object> options = validOptions();
        options.put("retry_backoff_ms", -1);

        SplunkConnectorException exception =
                Assertions.assertThrows(SplunkConnectorException.class, () -> configOf(options));
        Assertions.assertTrue(
                exception.getMessage().contains("'retry_backoff_ms' must not be negative"),
                exception.getMessage());
    }
}
