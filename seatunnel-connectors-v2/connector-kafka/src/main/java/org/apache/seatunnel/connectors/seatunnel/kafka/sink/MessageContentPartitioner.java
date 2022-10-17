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

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class MessageContentPartitioner implements Partitioner {
    private static List<String> ASSIGNPARTITIONS;

    public static void setAssignPartitions(List<String> assignPartitionList) {
        ASSIGNPARTITIONS = assignPartitionList;
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        int assignPartitionsSize = ASSIGNPARTITIONS.size();
        String message = new String(valueBytes);
        for (int i = 0; i < assignPartitionsSize; i++) {
            if (message.contains(ASSIGNPARTITIONS.get(i))) {
                return i;
            }
        }
        //Choose one of the remaining partitions according to the hashcode.
        return ((message.hashCode() & Integer.MAX_VALUE) % (numPartitions - assignPartitionsSize)) + assignPartitionsSize;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
