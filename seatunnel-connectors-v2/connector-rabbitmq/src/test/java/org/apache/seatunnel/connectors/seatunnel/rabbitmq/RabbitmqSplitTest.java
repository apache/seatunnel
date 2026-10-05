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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq;

import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RabbitmqSplitTest {

    /**
     * Test that the Split ID corresponds exactly to the Queue Name. In our Multi-table
     * architecture, each Split represents a unique RabbitMQ Queue.
     */
    @Test
    public void testSplitCreation() {
        String queueName = "my_test_queue";
        RabbitmqSplit split = new RabbitmqSplit(queueName);

        // The splitId MUST be the queue name to ensure correct mapping in the Reader.
        Assertions.assertEquals(queueName, split.splitId());
    }

    /**
     * Test equality logic. Two splits are considered equal if they target the same Queue Name. This
     * is crucial for the Enumerator to correctly assign/deduplicate splits.
     */
    @Test
    public void testSplitEquality() {
        RabbitmqSplit split1 = new RabbitmqSplit("queue_A");
        RabbitmqSplit split2 = new RabbitmqSplit("queue_A");
        RabbitmqSplit split3 = new RabbitmqSplit("queue_B");

        // Reflexive
        Assertions.assertEquals(split1, split1);

        // Symmetric (Same queue name -> Equal)
        Assertions.assertEquals(split1, split2);
        Assertions.assertEquals(split2, split1);

        // Different queue name -> Not Equal
        Assertions.assertNotEquals(split1, split3);
        Assertions.assertNotEquals(null, split1);
        Assertions.assertNotEquals(split1, new Object());
    }

    /**
     * Test HashCode consistency. If two objects are equal according to .equals(), they must have
     * the same hash code. This is required because SeaTunnel uses Sets/Maps to manage active
     * splits.
     */
    @Test
    public void testSplitHashCode() {
        RabbitmqSplit split1 = new RabbitmqSplit("queue_A");
        RabbitmqSplit split2 = new RabbitmqSplit("queue_A");

        // Since split1 equals split2, their hashCodes MUST be identical.
        Assertions.assertEquals(split1.hashCode(), split2.hashCode());

        // Ensure different queues likely produce different hashCodes
        RabbitmqSplit split3 = new RabbitmqSplit("queue_B");
        Assertions.assertNotEquals(split1.hashCode(), split3.hashCode());
    }

    @Test
    public void testSerialization() throws java.io.IOException, ClassNotFoundException {
        // 1. Create a Split
        String originalQueue = "important_queue";
        RabbitmqSplit originalSplit = new RabbitmqSplit(originalQueue);

        // 2. Serialize (Write to byte array)
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
        oos.writeObject(originalSplit);
        oos.close();

        // 3. Deserialize (Read from byte array)
        java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(baos.toByteArray());
        java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bais);
        RabbitmqSplit restoredSplit = (RabbitmqSplit) ois.readObject();

        // 4. Verify
        Assertions.assertEquals(originalSplit.splitId(), restoredSplit.splitId());
        Assertions.assertEquals(originalQueue, restoredSplit.splitId());
        Assertions.assertEquals(originalSplit, restoredSplit);
    }
}
