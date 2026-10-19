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

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KafkaNoTransactionSenderTest {

    private final MockProducer<byte[], byte[]> producer =
            new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
    private final KafkaNoTransactionSender<byte[], byte[]> sender =
            new KafkaNoTransactionSender<>(producer);

    private static ProducerRecord<byte[], byte[]> record() {
        return new ProducerRecord<>("topic", new byte[] {1});
    }

    @Test
    void testSnapshotStateSucceedsWhenAllSendsSucceed() {
        sender.send(record());
        sender.send(record());

        Assertions.assertTrue(sender.snapshotState(1L).isEmpty());
        Assertions.assertEquals(2, producer.history().size());
    }

    @Test
    void testFailedSendFailsTheCheckpoint() {
        TimeoutException cause = new TimeoutException("Topic topic not present in metadata");
        sender.send(record());
        producer.errorNext(cause);

        KafkaConnectorException e =
                Assertions.assertThrows(
                        KafkaConnectorException.class, () -> sender.snapshotState(1L));
        Assertions.assertSame(cause, e.getCause());
        assertErrorCode(KafkaConnectorErrorCode.PRODUCE_DATA_FAILED, e);
    }

    @Test
    void testFailedSendRejectsLaterRecords() {
        sender.send(record());
        producer.errorNext(new RecordTooLargeException("too large"));

        Assertions.assertThrows(KafkaConnectorException.class, () -> sender.send(record()));
        Assertions.assertEquals(1, producer.history().size());
    }

    @Test
    void testFirstSendFailureIsKeptAsTheCause() {
        RecordTooLargeException first = new RecordTooLargeException("first");
        sender.send(record());
        sender.send(record());
        producer.errorNext(first);
        producer.errorNext(new TimeoutException("second"));

        KafkaConnectorException e =
                Assertions.assertThrows(
                        KafkaConnectorException.class, () -> sender.snapshotState(1L));
        Assertions.assertSame(first, e.getCause());
    }

    private static void assertErrorCode(SeaTunnelErrorCode expected, KafkaConnectorException e) {
        Assertions.assertTrue(
                e.getMessage().contains(expected.getCode()),
                "expected " + expected.getCode() + " in: " + e.getMessage());
    }
}
