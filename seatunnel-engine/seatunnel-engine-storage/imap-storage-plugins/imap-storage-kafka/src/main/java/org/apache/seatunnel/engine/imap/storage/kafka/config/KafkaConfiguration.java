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

package org.apache.seatunnel.engine.imap.storage.kafka.config;

import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Builder
@Data
public class KafkaConfiguration implements Serializable {
    private String bootstrapServers;
    private String storageTopic;
    private Integer storageTopicReplicationFactor;
    private Integer storageTopicPartition;
    private Map<String, Object> producerConfigs;
    private Map<String, Object> consumerConfigs;
    private Map<String, Object> adminConfigs;
    private Map<String, Object> topicConfigs;

    public static Map<String, Object> setExtraConfiguration(
            Map<String, Object> config, String prefix) {
        Map<String, Object> extraConfigs = Maps.newConcurrentMap();
        config.forEach(
                (k, v) -> {
                    if (k.startsWith(prefix)) {
                        extraConfigs.put(k.replace(prefix, ""), config.get(k));
                    }
                });
        return extraConfigs;
    }
}
