/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.kafka.utils;

import org.apache.seatunnel.engine.imap.storage.kafka.config.KafkaConfiguration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.utils.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;

@Slf4j
public class TopicAdmin implements AutoCloseable {
    private final KafkaConfiguration kafkaConfiguration;
    private final Admin admin;

    public TopicAdmin(KafkaConfiguration config) {
        this.kafkaConfiguration = config;
        this.admin = createKafkaAdminClient();
    }

    private Admin createKafkaAdminClient() {
        Map<String, Object> configs = kafkaConfiguration.getAdminConfigs();
        configs.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaConfiguration.getBootstrapServers());
        return Admin.create(configs);
    }

    public void maybeCreateTopic(String topic) {
        Map<String, TopicDescription> existing = describeTopics(topic);
        if (!existing.isEmpty()) {
            log.info("Topic {} is existing, not create！！", topic);
            return;
        }
        log.info("Creating topic '{}'", topic);
        NewTopicBuilder newTopicBuilder =
                NewTopicBuilder.defineTopic(topic)
                        .config(kafkaConfiguration.getTopicConfigs())
                        .compacted()
                        .partitions(kafkaConfiguration.getStorageTopicPartition())
                        .replicationFactor(
                                kafkaConfiguration.getStorageTopicReplicationFactor().shortValue());
        NewTopic newTopic = newTopicBuilder.build();
        // Create the topic if it doesn't exist
        Set<String> newTopics = createTopics(newTopic);
        if (!newTopics.contains(topic)) {
            log.info(
                    "Using admin client to check cleanup policy of '{}' topic is '{}'",
                    topic,
                    TopicConfig.CLEANUP_POLICY_COMPACT);
            verifyTopicCleanupPolicyOnlyCompact(topic);
        }
    }

    /**
     * Create topics
     *
     * @param topics
     * @return
     */
    public Set<String> createTopics(NewTopic... topics) {
        Map<String, NewTopic> topicsByName = new HashMap<>();
        if (topics != null) {
            for (NewTopic topic : topics) {
                if (topic != null) {
                    topicsByName.put(topic.name(), topic);
                }
            }
        }
        if (topicsByName.isEmpty()) {
            return Collections.emptySet();
        }
        String bootstrapServers = kafkaConfiguration.getBootstrapServers();
        String topicNameList = Utils.join(topicsByName.keySet(), "', '");

        Map<String, KafkaFuture<Void>> results =
                admin.createTopics(
                                topicsByName.values(),
                                new CreateTopicsOptions().validateOnly(false))
                        .values();

        Set<String> createdTopicNames = new HashSet<>();
        for (Map.Entry<String, KafkaFuture<Void>> entry : results.entrySet()) {
            String topic = entry.getKey();
            try {
                entry.getValue().get();
                createdTopicNames.add(topic);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof TopicExistsException) {
                    log.info(
                            "Found existing topic '{}' on the brokers at {}",
                            topic,
                            bootstrapServers);
                    createdTopicNames.add(topic);
                    continue;
                }
                if (cause instanceof UnsupportedVersionException) {
                    log.error(
                            "Unable to create topic(s) '{}' since the brokers at {} do not support the CreateTopics API.",
                            topicNameList,
                            bootstrapServers,
                            cause);
                    return Collections.emptySet();
                }
                if (cause instanceof ClusterAuthorizationException) {
                    log.info(
                            "Not authorized to create topic(s) '{}' upon the brokers {}.",
                            topicNameList,
                            bootstrapServers);
                    return Collections.emptySet();
                }
                if (cause instanceof TopicAuthorizationException) {
                    log.info(
                            "Not authorized to create topic(s) '{}' upon the brokers {}.",
                            topicNameList,
                            bootstrapServers);
                    return Collections.emptySet();
                }
                if (cause instanceof InvalidConfigurationException) {
                    throw new RuntimeException(
                            "Unable to create topic(s) '"
                                    + topicNameList
                                    + "': "
                                    + cause.getMessage(),
                            cause);
                }
                throw new RuntimeException(
                        "Error while attempting to create/find topic(s) '" + topicNameList + "'",
                        e);
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new RuntimeException(
                        "Interrupted while attempting to create/find topic(s) '"
                                + topicNameList
                                + "'",
                        e);
            }
        }
        return createdTopicNames;
    }

    /**
     * Verify topic cleanup policy
     *
     * @param topic
     */
    public void verifyTopicCleanupPolicyOnlyCompact(String topic) {
        Set<String> cleanupPolicies = topicCleanupPolicy(topic);
        if (cleanupPolicies.isEmpty()) {
            log.info(
                    "Unable to use admin client to verify the cleanup policy of '{}' "
                            + "topic is '{}', either because does not have the required permission to "
                            + "describe topic configurations.Please ensure that the cleanup.policy policy is compact",
                    topic,
                    TopicConfig.CLEANUP_POLICY_COMPACT);
            return;
        }
        Set<String> expectedPolicies = Collections.singleton(TopicConfig.CLEANUP_POLICY_COMPACT);
        if (!cleanupPolicies.equals(expectedPolicies)) {
            String expectedPolicyStr = String.join(",", expectedPolicies);
            String cleanupPolicyStr = String.join(",", cleanupPolicies);
            String msg =
                    String.format(
                            "The '%s' configuration for Topic '%s' must be '%s', but the "
                                    + "current configuration is  '%s', which may cause data loss",
                            TopicConfig.CLEANUP_POLICY_CONFIG,
                            topic,
                            expectedPolicyStr,
                            cleanupPolicyStr);
            throw new ConfigException(msg);
        }
    }

    public Set<String> topicCleanupPolicy(String topic) {
        Config topicConfig = describeTopicConfigs(topic).get(topic);
        if (topicConfig == null) {
            log.warn("Unable to find topic '{}' when getting cleanup policy", topic);
            return Collections.emptySet();
        }
        ConfigEntry entry = topicConfig.get(CLEANUP_POLICY_CONFIG);
        if (entry != null && entry.value() != null) {
            String policy = entry.value();
            log.info("Found cleanup.policy = {} for topic '{}'", policy, topic);
            return Arrays.stream(policy.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(String::toLowerCase)
                    .collect(Collectors.toSet());
        }
        log.debug("Found no cleanup.policy for topic '{}'", topic);
        return Collections.emptySet();
    }

    public Map<String, Config> describeTopicConfigs(String... topicNames) {
        if (topicNames == null) {
            return Collections.emptyMap();
        }
        Collection<String> topics =
                Arrays.stream(topicNames)
                        .filter(Objects::nonNull)
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());
        if (topics.isEmpty()) {
            return Collections.emptyMap();
        }
        String bootstrapServers = kafkaConfiguration.getBootstrapServers();
        String topicNameList = topics.stream().collect(Collectors.joining(", "));
        Collection<ConfigResource> resources =
                topics.stream()
                        .map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t))
                        .collect(Collectors.toList());

        Map<ConfigResource, KafkaFuture<Config>> topicConfigs =
                admin.describeConfigs(resources, new DescribeConfigsOptions()).values();

        Map<String, Config> result = new HashMap<>();
        for (Map.Entry<ConfigResource, KafkaFuture<Config>> entry : topicConfigs.entrySet()) {
            ConfigResource resource = entry.getKey();
            KafkaFuture<Config> configs = entry.getValue();
            String topic = resource.name();
            try {
                result.put(topic, configs.get());
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof UnknownTopicOrPartitionException) {
                    log.info(
                            "Topic '{}' does not exist on the brokers at {}",
                            topic,
                            bootstrapServers);
                    result.put(topic, null);
                } else if (cause instanceof ClusterAuthorizationException
                        || cause instanceof TopicAuthorizationException) {
                    log.warn(
                            "Not authorized to describe topic config for topic '{}' on brokers at {}",
                            topic,
                            bootstrapServers);
                } else if (cause instanceof UnsupportedVersionException) {
                    log.warn(
                            "API to describe topic config for topic '{}' is unsupported on brokers at {}",
                            topic,
                            bootstrapServers);

                } else {
                    String msg =
                            String.format(
                                    "Error while attempting to describe topic config for topic '%s' on brokers at %s",
                                    topic, bootstrapServers);
                    throw new RuntimeException(msg, e);
                }
            } catch (InterruptedException e) {
                Thread.interrupted();
                String msg =
                        String.format(
                                "Interrupted while attempting to describe topic configs '%s'",
                                topicNameList);
                throw new RuntimeException(msg, e);
            }
        }
        return result;
    }

    public Map<String, TopicDescription> describeTopics(String... topics) {
        if (topics == null) {
            return Collections.emptyMap();
        }
        String bootstrapServers = kafkaConfiguration.getBootstrapServers();
        String topicNames = String.join(", ", topics);

        Map<String, KafkaFuture<TopicDescription>> newResults =
                admin.describeTopics(Arrays.asList(topics), new DescribeTopicsOptions()).values();

        Map<String, TopicDescription> existingTopics = new HashMap<>();
        newResults.forEach(
                (topic, desc) -> {
                    try {
                        existingTopics.put(topic, desc.get());
                    } catch (ExecutionException e) {
                        Throwable cause = e.getCause();
                        if (cause instanceof UnknownTopicOrPartitionException) {
                            log.debug(
                                    "Topic '{}' does not exist on the brokers at {}",
                                    topic,
                                    bootstrapServers);
                            return;
                        }
                        if (cause instanceof ClusterAuthorizationException
                                || cause instanceof TopicAuthorizationException) {
                            String msg =
                                    String.format(
                                            "Not authorized to describe topic(s) '%s' on the brokers %s",
                                            topicNames, bootstrapServers);
                            throw new RuntimeException(msg, cause);
                        }
                        if (cause instanceof UnsupportedVersionException) {
                            String msg =
                                    String.format(
                                            "Unable to describe topic(s) '%s' since the brokers "
                                                    + "at %s do not support the DescribeTopics API.",
                                            topicNames, bootstrapServers);
                            throw new RuntimeException(msg, cause);
                        }
                        throw new RuntimeException(
                                "Error while attempting to describe topics '" + topicNames + "'",
                                e);
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                        throw new RuntimeException(
                                "Interrupted while attempting to describe topics '"
                                        + topicNames
                                        + "'",
                                e);
                    }
                });
        return existingTopics;
    }

    public boolean deleteTopic(String topic) throws ExecutionException, InterruptedException {
        Set<String> deleteTopicsResult = deleteTopics(topic);
        return deleteTopicsResult.contains(topic);
    }

    public Set<String> deleteTopics(String... topics)
            throws ExecutionException, InterruptedException {
        Map<String, KafkaFuture<Void>> topicsResult =
                admin.deleteTopics(Lists.newArrayList(topics), new DeleteTopicsOptions())
                        .topicNameValues();
        Set<String> deleteTopics = Sets.newHashSet();
        for (Map.Entry<String, KafkaFuture<Void>> entry : topicsResult.entrySet()) {
            entry.getValue().get();
            deleteTopics.add(entry.getKey());
        }
        return deleteTopics;
    }

    @Override
    public void close() throws Exception {
        this.admin.close();
    }
}
