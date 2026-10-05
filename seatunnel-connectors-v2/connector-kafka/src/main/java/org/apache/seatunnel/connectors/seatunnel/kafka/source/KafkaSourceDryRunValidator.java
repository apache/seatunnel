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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.common.utils.TemporaryClassLoaderContext;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/** Metadata-only validation, isolated from the Kafka source runtime and its offset handling. */
final class KafkaSourceDryRunValidator {

    private static final int MAX_TIMEOUT_MS = 30_000;

    private KafkaSourceDryRunValidator() {}

    static void validate(KafkaSourceConfig config) throws Exception {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Kafka connect dry-run interrupted");
        }
        Set<String> topics = new HashSet<>();
        List<Pattern> patterns = new ArrayList<>();
        for (ConsumerMetadata metadata : config.getMapMetadata().values()) {
            if (metadata.isPattern()) {
                patterns.add(Pattern.compile(metadata.getTopic()));
            } else {
                topics.addAll(Arrays.asList(metadata.getTopic().split(",")));
            }
        }
        if (topics.isEmpty() && patterns.isEmpty()) {
            throw new IllegalArgumentException("Kafka connect dry-run requires at least one topic");
        }

        Properties properties = new Properties();
        properties.putAll(config.getProperties());
        // Match the runtime enumerator: the connector bootstrap option takes precedence.
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrap());
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(KafkaSourceDryRunValidator.class.getClassLoader())) {
            AdminClientConfig adminConfig = new AdminClientConfig(properties);
            int apiTimeoutMs = adminConfig.getInt(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
            int requestTimeoutMs = adminConfig.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
            // Preserve Kafka's original validation before applying dry-run limits: an explicit API
            // timeout below the request timeout also fails during normal source startup.
            if (properties.containsKey(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)
                    && apiTimeoutMs < requestTimeoutMs) {
                throw new ConfigException(
                        AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG
                                + " must not be smaller than "
                                + AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
            }
            int timeoutMs = Math.min(MAX_TIMEOUT_MS, Math.max(apiTimeoutMs, requestTimeoutMs));
            properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs);
            properties.put(
                    AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                    Math.min(timeoutMs, requestTimeoutMs));

            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            AdminClient admin = AdminClient.create(properties);
            try {
                if (!patterns.isEmpty()) {
                    Set<String> visibleTopics =
                            await(
                                    admin.listTopics(
                                                    new ListTopicsOptions()
                                                            .timeoutMs(remainingMillis(deadline)))
                                            .names(),
                                    deadline);
                    for (String topic : visibleTopics) {
                        if (patterns.stream()
                                .anyMatch(pattern -> pattern.matcher(topic).matches())) {
                            topics.add(topic);
                        }
                    }
                }
                // An empty pattern match is valid for a source waiting for future topics.
                if (!topics.isEmpty()) {
                    await(
                            admin.describeTopics(
                                            topics,
                                            new DescribeTopicsOptions()
                                                    .timeoutMs(remainingMillis(deadline)))
                                    .allTopicNames(),
                            deadline);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } finally {
                // Kafka 3.4 uses Thread.join(timeout): zero would mean an unbounded close wait.
                admin.close(Duration.ofMillis(1));
            }
        }
    }

    private static int remainingMillis(long deadline) throws TimeoutException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new TimeoutException("Kafka connect dry-run metadata check timed out");
        }
        return (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining));
    }

    private static <T> T await(KafkaFuture<T> future, long deadline)
            throws ExecutionException, InterruptedException, TimeoutException {
        return future.get(remainingMillis(deadline), TimeUnit.MILLISECONDS);
    }
}
