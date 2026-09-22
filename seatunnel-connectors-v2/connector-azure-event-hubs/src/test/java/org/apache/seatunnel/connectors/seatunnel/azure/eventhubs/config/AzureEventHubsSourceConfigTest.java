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
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.source.AzureEventHubsSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.source.AzureEventHubsSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class AzureEventHubsSourceConfigTest {

    private static final String SAS_KEY = "c3ludGhldGljLXNlY3JldA==";
    private static final String CONNECTION_STRING =
            "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey="
                    + SAS_KEY;

    @Test
    void entityPathRejectionAndStringRepresentationsDoNotExposeCredentials() {
        Map<String, Object> options = validOptions();
        options.put("connection_string", CONNECTION_STRING);
        AzureEventHubsSourceConfig config = config(options);
        Assertions.assertEquals(CONNECTION_STRING, config.getConnectionString());
        Assertions.assertFalse(config.toString().contains(SAS_KEY));
        Assertions.assertFalse(
                new AzureEventHubsSourceSplit(config.getEventHubName(), "0", 10L)
                        .toString()
                        .contains(SAS_KEY));
        for (String hub : new String[] {"events", "different-hub"}) {
            options.put("connection_string", CONNECTION_STRING + ";EnTiTyPaTh=" + hub);
            OptionValidationException exception =
                    Assertions.assertThrows(OptionValidationException.class, () -> config(options));
            Assertions.assertTrue(exception.getMessage().contains("must not include EntityPath"));
            Assertions.assertFalse(ExceptionUtils.getMessage(exception).contains(SAS_KEY));
            Assertions.assertFalse(
                    ExceptionUtils.getMessage(exception).contains(CONNECTION_STRING));
            Assertions.assertNull(exception.getCause());
        }
    }

    @Test
    void defaultsAreBoundedAndStartFromEarliest() {
        AzureEventHubsSourceConfig config = config(validOptions());

        Assertions.assertEquals("events", config.getEventHubName());
        Assertions.assertEquals("$Default", config.getConsumerGroup());
        Assertions.assertEquals(AzureEventHubsStartMode.EARLIEST, config.getStartMode());
        Assertions.assertEquals(AzureEventHubsMessageFormat.JSON, config.getFormat());
        Assertions.assertEquals(100, config.getMaxBatchSize());
        Assertions.assertEquals(1_000L, config.getPollTimeoutMs());
        Assertions.assertEquals(300, config.getPrefetchCount());
    }

    @Test
    void rejectsBlankRequiredOptions() {
        for (String option :
                new String[] {"connection_string", "event_hub_name", "consumer_group"}) {
            Map<String, Object> options = validOptions();
            options.put(option, "  ");

            OptionValidationException exception =
                    Assertions.assertThrows(OptionValidationException.class, () -> config(options));
            Assertions.assertTrue(exception.getMessage().contains(option));
        }
    }

    @Test
    void rejectsEntityPathInNamespaceConnectionStringCaseInsensitively() {
        Map<String, Object> options = validOptions();
        options.put(
                "connection_string",
                "Endpoint=sb://example/;SharedAccessKeyName=name;SharedAccessKey=key;EnTiTyPaTh=events;");

        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> config(options));

        Assertions.assertTrue(exception.getMessage().contains("must not include EntityPath"));
    }

    @Test
    void permitsEntityPathTextInsideCredentialValues() {
        Map<String, Object> options = validOptions();
        options.put(
                "connection_string",
                "Endpoint=sb://example/;SharedAccessKeyName=EntityPathUser;SharedAccessKey=key;");

        Assertions.assertDoesNotThrow(() -> config(options));
    }

    @Test
    void textFormatRequiresNonEmptyDelimiter() {
        Map<String, Object> options = validOptions();
        options.put("format", "text");
        options.put("field_delimiter", "");

        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> config(options));

        Assertions.assertTrue(exception.getMessage().contains("field_delimiter"));
    }

    @Test
    void rejectsNonPositiveCapacityOptions() {
        for (String option : new String[] {"max_batch_size", "prefetch_count"}) {
            Map<String, Object> options = validOptions();
            options.put(option, 0);

            OptionValidationException exception =
                    Assertions.assertThrows(OptionValidationException.class, () -> config(options));
            Assertions.assertTrue(exception.getMessage().contains(option));
        }
    }

    @Test
    void boundsPollTimeoutForReaderWakeupResponsiveness() {
        for (long value : new long[] {0L, AzureEventHubsSourceConfig.MAX_POLL_TIMEOUT_MS + 1L}) {
            Map<String, Object> options = validOptions();
            options.put("poll_timeout_ms", value);

            OptionValidationException exception =
                    Assertions.assertThrows(OptionValidationException.class, () -> config(options));
            Assertions.assertTrue(exception.getMessage().contains("'poll_timeout_ms' > 0"));
            Assertions.assertTrue(exception.getMessage().contains("'poll_timeout_ms' <= 5000"));
        }
    }

    @Test
    void acceptsSdkMaximumPrefetchCount() {
        Map<String, Object> options = validOptions();
        options.put("max_batch_size", 8000);
        options.put("prefetch_count", 8000);

        Assertions.assertEquals(8000, config(options).getPrefetchCount());
    }

    @Test
    void rejectsPrefetchCountAboveSdkMaximumWithSafeDiagnostic() {
        Map<String, Object> options = validOptions();
        options.put("prefetch_count", 8001);
        options.put(
                "connection_string",
                "Endpoint=sb://example/;SharedAccessKeyName=listen;SharedAccessKey=private-sas-key;");

        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> config(options));

        Assertions.assertTrue(exception.getMessage().contains("'prefetch_count' <= 8000"));
        Assertions.assertFalse(ExceptionUtils.getMessage(exception).contains("private-sas-key"));
        Assertions.assertNull(exception.getCause());
    }

    @Test
    void prefetchMustHoldAtLeastOneConfiguredBatch() {
        Map<String, Object> options = validOptions();
        options.put("max_batch_size", 101);
        options.put("prefetch_count", 100);

        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> config(options));

        Assertions.assertTrue(
                exception.getMessage().contains("'prefetch_count' >= 'max_batch_size'"));
    }

    @Test
    void factoryRulesRejectBlankAndCrossFieldOptions() {
        for (String option :
                new String[] {"connection_string", "event_hub_name", "consumer_group"}) {
            Map<String, Object> options = validOptions();
            options.put(option, "  ");
            assertFactoryRuleRejects(options, option);
        }

        Map<String, Object> options = validOptions();
        options.put("max_batch_size", 100);
        options.put("prefetch_count", 99);
        assertFactoryRuleRejects(options, "prefetch_count");
        options.put("max_batch_size", 99);
        validateFactoryRules(options);
    }

    @Test
    void runtimeCrossFieldValidationIncludesDefaults() {
        Map<String, Object> largerBatch = validOptions();
        largerBatch.put("max_batch_size", 301);
        Assertions.assertThrows(OptionValidationException.class, () -> config(largerBatch));
        Map<String, Object> smallerPrefetch = validOptions();
        smallerPrefetch.put("prefetch_count", 99);
        Assertions.assertThrows(OptionValidationException.class, () -> config(smallerPrefetch));
    }

    @Test
    void factoryBoundsDoNotDependOnExplicitBatchSize() {
        for (int prefetch : new int[] {0, 8001}) {
            Map<String, Object> options = validOptions();
            options.put("connection_string", CONNECTION_STRING);
            options.put("prefetch_count", prefetch);
            OptionValidationException exception =
                    assertFactoryRuleRejects(options, "prefetch_count");
            Assertions.assertFalse(ExceptionUtils.getMessage(exception).contains(SAS_KEY));
        }
    }

    @Test
    void factoryRulesRejectEntityPathWithoutExposingCredentials() {
        for (String suffix :
                new String[] {";EnTiTyPaTh=events", "; EntityPath =other", ";EntityPath="}) {
            Map<String, Object> options = validOptions();
            options.put("connection_string", CONNECTION_STRING + suffix);
            OptionValidationException exception =
                    assertFactoryRuleRejects(options, "must not include EntityPath");
            Assertions.assertFalse(ExceptionUtils.getMessage(exception).contains(SAS_KEY));
            Assertions.assertFalse(
                    ExceptionUtils.getMessage(exception).contains(CONNECTION_STRING));
            Assertions.assertNull(exception.getCause());
            Assertions.assertEquals(0, exception.getSuppressed().length);
        }

        Map<String, Object> options = validOptions();
        options.put("connection_string", CONNECTION_STRING.replace("=listen;", "=EntityPathUser;"));
        validateFactoryRules(options);
    }

    @Test
    void factoryRulesPreserveFormatSpecificDelimiterSemantics() {
        Map<String, Object> options = validOptions();
        options.put("field_delimiter", "");
        validateFactoryRules(options);
        options.put("format", "text");
        assertFactoryRuleRejects(options, "field_delimiter");
        options.put("field_delimiter", " ");
        validateFactoryRules(options);
    }

    private OptionValidationException assertFactoryRuleRejects(
            Map<String, Object> options, String expectedMessage) {
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateFactoryRules(options));
        Assertions.assertTrue(
                exception.getMessage().contains(expectedMessage), exception.getMessage());
        return exception;
    }

    private void validateFactoryRules(Map<String, Object> options) {
        options.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("value", "string")));
        ConfigValidator.of(ReadonlyConfig.fromMap(options))
                .validate(new AzureEventHubsSourceFactory().optionRule());
    }

    private AzureEventHubsSourceConfig config(Map<String, Object> options) {
        return AzureEventHubsSourceConfig.from(ReadonlyConfig.fromMap(options));
    }

    private Map<String, Object> validOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put(
                "connection_string",
                "Endpoint=sb://example/;SharedAccessKeyName=name;SharedAccessKey=key;");
        options.put("event_hub_name", "events");
        return options;
    }
}
