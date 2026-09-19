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

package org.apache.seatunnel.connectors.seatunnel.kafka.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.TemporaryClassLoaderContext;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormat;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.BOOTSTRAP_SERVERS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.KAFKA_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.PARTITION;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.TOPIC;

/** Metadata-only sink validation; never creates a producer or initializes a transaction. */
final class KafkaSinkDryRunValidator {

    private static final int MAX_TIMEOUT_MS = 30_000;
    private static final Pattern TOPIC_FIELD = Pattern.compile("\\$\\{(.*?)\\}", Pattern.DOTALL);

    private KafkaSinkDryRunValidator() {}

    static void validate(ReadonlyConfig config) throws Exception {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Kafka sink connect dry-run interrupted");
        }
        if (config.get(FORMAT) != MessageFormat.NATIVE
                && config.get(PARTITION) != null
                && config.get(PARTITION) < 0) {
            throw new IllegalArgumentException(
                    "Kafka sink connect dry-run: partition must not be negative");
        }
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(KafkaSinkDryRunValidator.class.getClassLoader())) {
            Properties properties = new Properties();
            if (config.get(KAFKA_CONFIG) != null) {
                properties.putAll(config.get(KAFKA_CONFIG));
            }
            // Match the writer: the top-level option overrides kafka.config.
            properties.put(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.get(BOOTSTRAP_SERVERS));
            AdminClientConfig adminConfig = new AdminClientConfig(properties);
            int apiTimeout = adminConfig.getInt(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
            int requestTimeout = adminConfig.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
            if (properties.containsKey(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)
                    && apiTimeout < requestTimeout) {
                throw new ConfigException("Invalid Kafka metadata timeouts");
            }
            int timeout = Math.min(MAX_TIMEOUT_MS, Math.max(apiTimeout, requestTimeout));
            properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeout);
            properties.put(
                    AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.min(timeout, requestTimeout));
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
            AdminClient admin = AdminClient.create(properties);
            try {
                String topic = config.get(TOPIC);
                if (TOPIC_FIELD.matcher(topic).find()) {
                    // The actual topic is a record value, not an interpolated config string.
                    await(
                            admin.describeCluster(
                                            new DescribeClusterOptions()
                                                    .timeoutMs(remainingMillis(deadline)))
                                    .nodes(),
                            deadline);
                } else {
                    TopicDescription description =
                            await(
                                            admin.describeTopics(
                                                            Collections.singleton(topic),
                                                            new DescribeTopicsOptions()
                                                                    .timeoutMs(
                                                                            remainingMillis(
                                                                                    deadline)))
                                                    .allTopicNames(),
                                            deadline)
                                    .get(topic);
                    // NATIVE uses the record's partition and ignores the top-level option.
                    Integer partition =
                            config.get(FORMAT) == MessageFormat.NATIVE
                                    ? null
                                    : config.get(PARTITION);
                    if (partition != null
                            && description.partitions().stream()
                                    .noneMatch(info -> info.partition() == partition)) {
                        throw new IllegalArgumentException(
                                "Kafka sink connect dry-run: partition is outside the target topic's range");
                    }
                }
            } finally {
                // Kafka uses Thread.join(timeout); zero could wait indefinitely.
                admin.close(Duration.ofMillis(1));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedException("Kafka sink connect dry-run interrupted");
        } catch (Exception e) {
            Throwable failure =
                    e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
            String reason =
                    "metadata validation failed; check topic, partition and Kafka client configuration";
            if (failure instanceof AuthenticationException) {
                reason = "authentication failed";
            } else if (failure instanceof AuthorizationException) {
                reason = "metadata authorization failed";
            } else if (failure instanceof UnknownTopicOrPartitionException) {
                reason = "target topic does not exist";
            } else if (failure instanceof TimeoutException
                    || failure instanceof org.apache.kafka.common.errors.TimeoutException) {
                reason = "metadata request timed out";
            } else if (failure instanceof ConfigException) {
                reason = "invalid Kafka client configuration";
            }
            // Client exceptions can embed JAAS options, passwords and endpoint credentials.
            throw new IllegalArgumentException("Kafka sink connect dry-run: " + reason);
        }
    }

    private static int remainingMillis(long deadline) throws TimeoutException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new TimeoutException();
        }
        return (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining));
    }

    private static <T> T await(KafkaFuture<T> future, long deadline) throws Exception {
        return future.get(remainingMillis(deadline), TimeUnit.MILLISECONDS);
    }
}
