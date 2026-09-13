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

class GooglePubSubSourceConfigTest {

    @Test
    void shouldRejectCredentialsForEmulator() {
        Map<String, Object> options = requiredOptions();
        options.put("credentials_path", "service-account.json");
        options.put("emulator_host", "localhost:8085");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> GooglePubSubSourceConfig.from(ReadonlyConfig.fromMap(options)));
        Assertions.assertTrue(exception.getMessage().contains("cannot be configured together"));
    }

    @Test
    void shouldRejectBlankSubscription() {
        Map<String, Object> options = requiredOptions();
        options.put("subscription", " ");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> GooglePubSubSourceConfig.from(ReadonlyConfig.fromMap(options)));
        Assertions.assertTrue(exception.getMessage().contains("subscription"));
    }

    @Test
    void shouldRejectEmptyTextDelimiter() {
        Map<String, Object> options = requiredOptions();
        options.put("format", "text");
        options.put("field_delimiter", "");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> GooglePubSubSourceConfig.from(ReadonlyConfig.fromMap(options)));
        Assertions.assertTrue(exception.getMessage().contains("field_delimiter"));
    }

    @Test
    void shouldRejectNonPositiveFlowControlOptions() {
        for (String option :
                new String[] {
                    "max_outstanding_messages", "max_outstanding_bytes", "parallel_pull_count"
                }) {
            Map<String, Object> options = requiredOptions();
            options.put(option, 0);

            IllegalArgumentException exception =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () -> GooglePubSubSourceConfig.from(ReadonlyConfig.fromMap(options)));
            Assertions.assertTrue(exception.getMessage().contains(option));
        }
    }

    @Test
    void shouldReadFlowControlOptions() {
        Map<String, Object> options = requiredOptions();
        options.put("max_outstanding_messages", 100L);
        options.put("max_outstanding_bytes", 1024L);
        options.put("parallel_pull_count", 2);

        GooglePubSubSourceConfig config =
                GooglePubSubSourceConfig.from(ReadonlyConfig.fromMap(options));

        Assertions.assertEquals(100L, config.getMaxOutstandingMessages());
        Assertions.assertEquals(1024L, config.getMaxOutstandingBytes());
        Assertions.assertEquals(2, config.getParallelPullCount());
    }

    private Map<String, Object> requiredOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("project_id", "test-project");
        options.put("subscription", "test-subscription");
        return options;
    }
}
