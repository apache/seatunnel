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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class KafkaSourceSplitEnumeratorTest {

    @Test
    void addSplitsBack() {
        // prepare
        TopicPartition partition = new TopicPartition("test", 0);

        AdminClient adminClient = Mockito.mock(KafkaAdminClient.class);
        Mockito.when(adminClient.listOffsets(Mockito.any(java.util.Map.class)))
                .thenReturn(
                        new ListOffsetsResult(
                                new HashMap<
                                        TopicPartition,
                                        KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>>() {
                                    {
                                        put(
                                                partition,
                                                KafkaFuture.completedFuture(
                                                        new ListOffsetsResult.ListOffsetsResultInfo(
                                                                0, 0, Optional.of(0))));
                                    }
                                }));

        // test
        Map<TopicPartition, KafkaSourceSplit> assignedSplit =
                new HashMap<TopicPartition, KafkaSourceSplit>() {
                    {
                        put(partition, new KafkaSourceSplit(null, partition));
                    }
                };
        Map<TopicPartition, KafkaSourceSplit> pendingSplit = new HashMap<>();
        List<KafkaSourceSplit> splits = Arrays.asList(new KafkaSourceSplit(null, partition));
        KafkaSourceSplitEnumerator enumerator =
                new KafkaSourceSplitEnumerator(adminClient, pendingSplit, assignedSplit);
        enumerator.addSplitsBack(splits, 1);
        Assertions.assertTrue(pendingSplit.size() == splits.size());
        Assertions.assertNull(assignedSplit.get(partition));
    }
}
