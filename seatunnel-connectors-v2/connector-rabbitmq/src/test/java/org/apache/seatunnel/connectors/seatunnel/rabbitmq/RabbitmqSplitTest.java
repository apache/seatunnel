/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.rabbitmq;

import org.apache.seatunnel.shade.org.apache.commons.lang3.SerializationUtils;

import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class RabbitmqSplitTest {

    @Test
    public void testSplitSerialization() {
        RabbitmqSplit originalSplit = new RabbitmqSplit("split-1", "queue_users");

        byte[] bytes = SerializationUtils.serialize(originalSplit);
        RabbitmqSplit deserializedSplit = SerializationUtils.deserialize(bytes);

        Assertions.assertEquals(originalSplit.splitId(), deserializedSplit.splitId());
        Assertions.assertEquals(originalSplit.getQueueName(), deserializedSplit.getQueueName());
    }

    @Test
    public void testSplitWithCheckpointData() {
        RabbitmqSplit originalSplit =
                new RabbitmqSplit(
                        "split-2",
                        "queue_orders",
                        Collections.singletonList(100L),
                        Collections.singleton("corr-123"));

        byte[] bytes = SerializationUtils.serialize(originalSplit);
        RabbitmqSplit deserializedSplit = SerializationUtils.deserialize(bytes);

        Assertions.assertEquals("queue_orders", deserializedSplit.getQueueName());
        Assertions.assertEquals(1, deserializedSplit.getDeliveryTags().size());
        Assertions.assertEquals(100L, deserializedSplit.getDeliveryTags().get(0));
        Assertions.assertTrue(deserializedSplit.getCorrelationIds().contains("corr-123"));
    }
}
