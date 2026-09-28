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

package org.apache.seatunnel.connectors.seatunnel.google.pubsub.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class GooglePubSubSinkConfigTest {

    @Test
    void shouldRejectCredentialsForEmulator() {
        Map<String, Object> options = requiredOptions();
        options.put("credentials_path", "/tmp/service-account.json");
        options.put("emulator_host", "localhost:8085");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> GooglePubSubSinkConfig.from(ReadonlyConfig.fromMap(options)));
        Assertions.assertTrue(exception.getMessage().contains("cannot be configured together"));
    }

    @Test
    void shouldRejectBlankProjectId() {
        Map<String, Object> options = requiredOptions();
        options.put("project_id", " ");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> GooglePubSubSinkConfig.from(ReadonlyConfig.fromMap(options)));
        Assertions.assertTrue(exception.getMessage().contains("project_id"));
    }

    @Test
    void shouldRejectEmptyTextDelimiter() {
        Map<String, Object> options = requiredOptions();
        options.put("format", "text");
        options.put("field_delimiter", "");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> GooglePubSubSinkConfig.from(ReadonlyConfig.fromMap(options)));
        Assertions.assertTrue(exception.getMessage().contains("field_delimiter"));
    }

    private Map<String, Object> requiredOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("project_id", "test-project");
        options.put("topic", "test-topic");
        return options;
    }
}
