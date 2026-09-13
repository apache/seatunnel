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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class AzureQueueSourceConfigTest {

    @Test
    void shouldCreateConnectionStringConfiguration() {
        AzureQueueSourceConfig config = config(validOptions());

        Assertions.assertEquals("events", config.getQueueName());
        Assertions.assertEquals(
                AuthenticationType.CONNECTION_STRING, config.getAuthenticationType());
        Assertions.assertEquals(32, config.getBatchSize());
        Assertions.assertEquals(300, config.getVisibilityTimeoutSeconds());
    }

    @Test
    void shouldRejectBatchOutsideAzureLimit() {
        Map<String, Object> options = validOptions();
        options.put("batch_size", 33);

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));
        Assertions.assertTrue(exception.getMessage().contains("between 1 and 32"));
    }

    @Test
    void shouldRequireEnoughInFlightCapacityForOneBatch() {
        Map<String, Object> options = validOptions();
        options.put("batch_size", 10);
        options.put("max_in_flight_messages", 9);

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));
        Assertions.assertTrue(exception.getMessage().contains("greater than or equal"));
    }

    @Test
    void shouldKeepOperationTimeoutBelowVisibilityRenewalWindow() {
        Map<String, Object> options = validOptions();
        options.put("visibility_timeout_seconds", 60);
        options.put("operation_timeout_ms", 30_000L);

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));
        Assertions.assertTrue(exception.getMessage().contains("less than half"));
    }

    @Test
    void shouldReuseStrictAuthenticationValidation() {
        Map<String, Object> options = validOptions();
        options.put("endpoint", "https://account.queue.core.windows.net");

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));
        Assertions.assertTrue(exception.getMessage().contains("not valid"));
    }

    private AzureQueueSourceConfig config(Map<String, Object> options) {
        return AzureQueueSourceConfig.from(ReadonlyConfig.fromMap(options));
    }

    private Map<String, Object> validOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("queue_name", "events");
        options.put("authentication_type", "connection_string");
        options.put("connection_string", "UseDevelopmentStorage=true");
        return options;
    }
}
