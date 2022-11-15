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

import org.apache.seatunnel.connectors.seatunnel.kafka.config.StartMode;

import lombok.Data;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka consumer metadata, include topic, bootstrap server etc.
 */

@Data
public class ConsumerMetadata implements Serializable {

    private String topic;
    private boolean isPattern = false;
    private String bootstrapServers;
    private Properties properties;
    private String consumerGroup;
    private boolean commitOnCheckpoint = false;
    private StartMode startMode = StartMode.GROUP_OFFSETS;
    private Map<TopicPartition, Long> specificStartOffsets;
    private Long startOffsetsTimestamp;

}
