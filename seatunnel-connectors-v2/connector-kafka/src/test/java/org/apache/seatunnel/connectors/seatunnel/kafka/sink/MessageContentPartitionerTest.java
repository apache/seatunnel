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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MessageContentPartitionerTest {

    private static final String TOPIC = "test_topic";

    private Cluster createCluster(int numPartitions) {
        Node node = new Node(0, "localhost", 9092);
        PartitionInfo[] partitions = new PartitionInfo[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = new PartitionInfo(TOPIC, i, node, new Node[] {node}, new Node[] {node});
        }
        return new Cluster(
                "test-cluster",
                Collections.singletonList(node),
                Arrays.asList(partitions),
                Collections.emptySet(),
                Collections.emptySet());
    }

    private MessageContentPartitioner createPartitioner(List<String> assignPartitions) {
        MessageContentPartitioner partitioner = new MessageContentPartitioner();
        Map<String, Object> configs = new HashMap<>();
        configs.put(MessageContentPartitioner.ASSIGN_PARTITIONS_CONFIG, assignPartitions);
        partitioner.configure(configs);
        return partitioner;
    }

    @Test
    void testCrossInstanceIsolation() {
        // Two partitioners with different assign_partitions should not interfere
        MessageContentPartitioner partitionerA =
                createPartitioner(Arrays.asList("order", "payment"));
        MessageContentPartitioner partitionerB =
                createPartitioner(Arrays.asList("user", "product"));

        Cluster cluster = createCluster(4);

        // Partitioner A: "order" message should route to partition 0
        int partitionA =
                partitionerA.partition(
                        TOPIC, null, null, "order-123", "order-123".getBytes(), cluster);
        Assertions.assertEquals(0, partitionA);

        // Partitioner B: "user" message should route to partition 0 (its own assign list)
        int partitionB =
                partitionerB.partition(
                        TOPIC, null, null, "user-456", "user-456".getBytes(), cluster);
        Assertions.assertEquals(0, partitionB);

        // Partitioner A: "user" is NOT in A's list, should fall through to hash partition
        int partitionAForUser =
                partitionerA.partition(
                        TOPIC, null, null, "user-456", "user-456".getBytes(), cluster);
        // Must be in the remaining range [2, 3], not in assigned range [0, 1]
        Assertions.assertTrue(
                partitionAForUser >= 2 && partitionAForUser <= 3,
                "Partitioner A should not route 'user' to assigned partitions 0-1");
    }

    @Test
    void testAssignedPartitionRouting() {
        MessageContentPartitioner partitioner = createPartitioner(Arrays.asList("alpha", "beta"));
        Cluster cluster = createCluster(4);

        // "alpha" -> partition 0
        Assertions.assertEquals(
                0, partitioner.partition(TOPIC, null, null, "alpha", "alpha".getBytes(), cluster));

        // "beta" -> partition 1
        Assertions.assertEquals(
                1, partitioner.partition(TOPIC, null, null, "beta", "beta".getBytes(), cluster));

        // "contains alpha" -> partition 0 (contains match)
        Assertions.assertEquals(
                0,
                partitioner.partition(
                        TOPIC, null, null, "contains alpha", "contains alpha".getBytes(), cluster));
    }

    @Test
    void testUnassignedMessageFallbackToHash() {
        MessageContentPartitioner partitioner =
                createPartitioner(Collections.singletonList("reserved"));
        Cluster cluster = createCluster(4);

        // "other" is not in assign list, should hash to partition [1, 3]
        int partition =
                partitioner.partition(TOPIC, null, null, "other", "other".getBytes(), cluster);
        Assertions.assertTrue(
                partition >= 1 && partition <= 3,
                "Unassigned message should fall back to hash partition in range [1, 3]");
    }
}
