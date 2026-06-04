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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.client;

import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.source.DeliveryMessage;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.utility.Utility;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.HANDLE_SHUTDOWN_SIGNAL_FAILED;

/** Consumer that queues deliveries in a {@link BlockingQueue}. */
@Slf4j
public class QueueingConsumer extends DefaultConsumer {
    private final BlockingQueue<DeliveryMessage> queue;
    private final String splitId;

    // When this is non-null the queue is in shutdown mode and nextDelivery should
    // throw a shutdown signal exception.
    private volatile ShutdownSignalException shutdown;
    private volatile ConsumerCancelledException cancelled;
    private static final DeliveryMessage POISON = new DeliveryMessage(null, null);

    /**
     * Constructor for QueueingConsumer.
     *
     * @param channel rabbitmq channel
     * @param queue queue to store messages
     * @param splitId split id
     */
    public QueueingConsumer(Channel channel, BlockingQueue<DeliveryMessage> queue, String splitId) {
        super(channel);
        this.queue = queue;
        this.splitId = splitId;
    }

    private void checkShutdown() {
        if (shutdown != null) {
            throw Utility.fixStackTrace(shutdown);
        }
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        shutdown = sig;
        try {
            queue.put(POISON);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RabbitmqConnectorException(HANDLE_SHUTDOWN_SIGNAL_FAILED, e);
        }
    }

    @SneakyThrows
    @Override
    public void handleCancel(String consumerTag) throws IOException {
        cancelled = new ConsumerCancelledException();
        queue.put(POISON);
    }

    @SneakyThrows
    @Override
    public void handleDelivery(
            String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
            throws IOException {
        checkShutdown();
        Delivery delivery = new Delivery(envelope, properties, body);
        DeliveryMessage message = new DeliveryMessage(this.splitId, delivery);
        queue.put(message);
    }
}
