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

class AzureQueueSinkConfigTest {

    @Test
    void shouldAcceptConnectionStringAuthentication() {
        AzureQueueSinkConfig config = config(connectionStringOptions());

        Assertions.assertEquals(
                AuthenticationType.CONNECTION_STRING, config.getAuthenticationType());
        Assertions.assertEquals("events", config.getQueueName());
        Assertions.assertEquals(MessageEncoding.NONE, config.getMessageEncoding());
    }

    @Test
    void shouldAcceptSharedKeyAuthentication() {
        Map<String, Object> options = baseOptions("shared_key");
        options.put("endpoint", "https://example.queue.core.windows.net");
        options.put("account_name", "example");
        options.put("account_key", "key");

        AzureQueueSinkConfig config = config(options);

        Assertions.assertEquals(AuthenticationType.SHARED_KEY, config.getAuthenticationType());
    }

    @Test
    void shouldAcceptSasTokenAuthentication() {
        Map<String, Object> options = baseOptions("sas_token");
        options.put("endpoint", "https://example.queue.core.windows.net");
        options.put("sas_token", "?sv=test&sig=value");

        AzureQueueSinkConfig config = config(options);

        Assertions.assertEquals(AuthenticationType.SAS_TOKEN, config.getAuthenticationType());
    }

    @Test
    void shouldRejectCredentialsFromAnotherAuthenticationMode() {
        Map<String, Object> options = connectionStringOptions();
        options.put("account_name", "example");

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));

        Assertions.assertTrue(exception.getMessage().contains("account_name"));
    }

    @Test
    void shouldRejectInvalidQueueName() {
        Map<String, Object> options = connectionStringOptions();
        options.put("queue_name", "Invalid--Queue");

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));

        Assertions.assertTrue(exception.getMessage().contains("queue_name"));
    }

    @Test
    void shouldRejectEmptyTextDelimiter() {
        Map<String, Object> options = connectionStringOptions();
        options.put("format", "text");
        options.put("field_delimiter", "");

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));

        Assertions.assertTrue(exception.getMessage().contains("field_delimiter"));
    }

    @Test
    void shouldRejectNonPositiveAsyncLimits() {
        Map<String, Object> options = connectionStringOptions();
        options.put("max_in_flight", 0);

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));

        Assertions.assertTrue(exception.getMessage().contains("max_in_flight"));
    }

    private AzureQueueSinkConfig config(Map<String, Object> options) {
        return AzureQueueSinkConfig.from(ReadonlyConfig.fromMap(options));
    }

    private Map<String, Object> connectionStringOptions() {
        Map<String, Object> options = baseOptions("connection_string");
        options.put(
                "connection_string",
                "DefaultEndpointsProtocol=https;AccountName=example;AccountKey=key");
        return options;
    }

    private Map<String, Object> baseOptions(String authenticationType) {
        Map<String, Object> options = new HashMap<>();
        options.put("queue_name", "events");
        options.put("authentication_type", authenticationType);
        return options;
    }
}
