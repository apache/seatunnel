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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.topic;

import org.apache.pulsar.common.naming.TopicName;

import java.io.Serializable;
import java.util.Objects;

/**
 * Basic information about a topic. If the topic is not partitioned, the partition number will be -1.
 */
public class TopicPartition implements Serializable {
    private static final long serialVersionUID = 1L;

    private int hash = 0;
    /**
     * The topic name of the pulsar. It would be a full topic name, if your don't provide the tenant
     * and namespace, we would add them automatically.
     */
    private final String topic;

    /**
     * Index of partition for the topic. It would be natural number for partitioned topic with a
     * non-key_shared subscription.
     */
    private final int partition;

    public TopicPartition(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public String getFullTopicName() {
        if (partition < 0) {
            return topic;
        }
        return TopicName.get(topic).getPartition(partition).toString();
    }

    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        final int prime = 31;
        int result = 1;
        result = prime * result + partition;
        result = prime * result + Objects.hashCode(topic);
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TopicPartition other = (TopicPartition) obj;
        return partition == other.partition && Objects.equals(topic, other.topic);
    }
}
