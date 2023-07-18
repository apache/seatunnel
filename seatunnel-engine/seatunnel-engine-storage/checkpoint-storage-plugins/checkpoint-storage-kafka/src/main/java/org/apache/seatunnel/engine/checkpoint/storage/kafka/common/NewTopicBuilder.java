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

package org.apache.seatunnel.engine.checkpoint.storage.kafka.common;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG;

public class NewTopicBuilder {

    public static NewTopicBuilder defineTopic(String topicName) {
        return new NewTopicBuilder(topicName);
    }

    private final String name;
    private int numPartitions;
    private short replicationFactor;
    private final Map<String, String> configs = new HashMap<>();

    NewTopicBuilder(String name) {
        this.name = name;
    }

    public NewTopicBuilder partitions(int numPartitions) {
        this.numPartitions = numPartitions;
        return this;
    }

    public NewTopicBuilder replicationFactor(short replicationFactor) {
        this.replicationFactor = replicationFactor;
        return this;
    }

    public NewTopicBuilder compacted() {
        this.configs.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
        return this;
    }

    public NewTopicBuilder minInSyncReplicas(short minInSyncReplicas) {
        this.configs.put(
                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Short.toString(minInSyncReplicas));
        return this;
    }

    public NewTopicBuilder uncleanLeaderElection(boolean allow) {
        this.configs.put(UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, Boolean.toString(allow));
        return this;
    }

    public NewTopicBuilder config(Map<String, Object> configs) {
        if (configs != null) {
            for (Map.Entry<String, Object> entry : configs.entrySet()) {
                Object value = entry.getValue();
                this.configs.put(entry.getKey(), value != null ? value.toString() : null);
            }
        } else {
            // clear config
            this.configs.clear();
        }
        return this;
    }

    public NewTopic build() {
        return new NewTopic(name, Optional.of(numPartitions), Optional.of(replicationFactor))
                .configs(configs);
    }
}
