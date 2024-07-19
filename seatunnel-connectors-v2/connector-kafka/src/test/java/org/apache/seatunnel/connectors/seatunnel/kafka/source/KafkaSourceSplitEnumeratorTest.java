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

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaSourceSplitEnumeratorTest {

    @Test
    void resetOffset() {
        TopicPartition partition = new TopicPartition("test", 0);
        KafkaSourceSplit split = new KafkaSourceSplit(null, partition);
        Map<TopicPartition, KafkaSourceSplit> res =
                KafkaSourceSplitEnumerator.resetOffset(
                        Arrays.asList(split),
                        new HashMap<TopicPartition, Long>() {
                            {
                                put(partition, 0L);
                            }
                        });
        assertTrue(res.get(partition).isRecover());
    }

    @Test
    void shouldAssignSplit() {
        TopicPartition partition = new TopicPartition("test", 0);
        KafkaSourceSplit split = new KafkaSourceSplit(null, partition);
        // test none recovery
        Assertions.assertFalse(
                KafkaSourceSplitEnumerator.shouldAssignSplit(
                        new HashMap<TopicPartition, KafkaSourceSplit>() {
                            {
                                put(partition, split);
                            }
                        },
                        partition,
                        split));

        // test recovery
        split.setRecover(true);
        Assertions.assertTrue(
                KafkaSourceSplitEnumerator.shouldAssignSplit(
                        new HashMap<TopicPartition, KafkaSourceSplit>() {
                            {
                                put(partition, split);
                            }
                        },
                        partition,
                        split));
    }
}
