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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class AzureEventHubsSourceConfigTest {

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

            IllegalArgumentException exception =
                    Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));
            Assertions.assertTrue(exception.getMessage().contains(option));
        }
    }

    @Test
    void rejectsEntityPathInNamespaceConnectionStringCaseInsensitively() {
        Map<String, Object> options = validOptions();
        options.put(
                "connection_string",
                "Endpoint=sb://example/;SharedAccessKeyName=name;SharedAccessKey=key;EnTiTyPaTh=events;");

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));

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

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));

        Assertions.assertTrue(exception.getMessage().contains("field_delimiter"));
    }

    @Test
    void rejectsNonPositiveCapacityOptions() {
        for (String option : new String[] {"max_batch_size", "prefetch_count"}) {
            Map<String, Object> options = validOptions();
            options.put(option, 0);

            IllegalArgumentException exception =
                    Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));
            Assertions.assertTrue(exception.getMessage().contains(option));
        }
    }

    @Test
    void boundsPollTimeoutForReaderWakeupResponsiveness() {
        for (long value : new long[] {0L, AzureEventHubsSourceConfig.MAX_POLL_TIMEOUT_MS + 1L}) {
            Map<String, Object> options = validOptions();
            options.put("poll_timeout_ms", value);

            IllegalArgumentException exception =
                    Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));
            Assertions.assertTrue(exception.getMessage().contains("between 1 and"));
        }
    }

    @Test
    void prefetchMustHoldAtLeastOneConfiguredBatch() {
        Map<String, Object> options = validOptions();
        options.put("max_batch_size", 101);
        options.put("prefetch_count", 100);

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));

        Assertions.assertTrue(exception.getMessage().contains("greater than or equal"));
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
