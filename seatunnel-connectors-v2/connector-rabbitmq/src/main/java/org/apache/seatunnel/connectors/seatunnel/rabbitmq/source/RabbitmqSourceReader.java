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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.source;

import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.MESSAGE_ACK_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.MESSAGE_ACK_REJECTED;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.client.RabbitmqClient;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

@Slf4j
public class RabbitmqSourceReader<T> implements SourceReader<T, RabbitmqSplit> {
    protected final Handover<Delivery> handover;

    protected final SourceReader.Context context;
    protected transient Channel channel;
    private final boolean usesCorrelationId = true;
    protected transient boolean autoAck;

    protected transient Set<String> correlationIdsProcessedButNotAcknowledged;
    protected transient List<Long> deliveryTagsProcessedForCurrentSnapshot;

    protected final SortedMap<Long, List<Long>> pendingDeliveryTagsToCommit;
    protected final SortedMap<Long, Set<String>> pendingCorrelationIdsToCommit;

    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private RabbitmqClient rabbitMQClient;
    private DefaultConsumer consumer;
    private final RabbitmqConfig config;

    public RabbitmqSourceReader(DeserializationSchema<SeaTunnelRow> deserializationSchema,
                                SourceReader.Context context,
                                RabbitmqConfig config) {
        this.handover = new Handover<>();
        this.pendingDeliveryTagsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.pendingCorrelationIdsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.context = context;
        this.deserializationSchema = deserializationSchema;
        this.config = config;
        this.rabbitMQClient = new RabbitmqClient(config);
        this.channel = rabbitMQClient.getChannel();
    }

    @Override
    public void open() throws Exception {
        this.correlationIdsProcessedButNotAcknowledged = new HashSet<>();
        this.deliveryTagsProcessedForCurrentSnapshot = new ArrayList<>();
        consumer = rabbitMQClient.getQueueingConsumer(handover);

        if (Boundedness.UNBOUNDED.equals(context.getBoundedness())) {
            autoAck = false;
            // enables transaction mode
            channel.txSelect();
        } else {
            autoAck = true;
        }

        log.debug("Starting RabbitMQ source with autoAck status: " + autoAck);
        channel.basicConsume(config.getQueueName(), autoAck, consumer);
    }

    @Override
    public void close() throws IOException {
        if (rabbitMQClient != null) {
            rabbitMQClient.close();
        }
    }

    @Override
    public void pollNext(Collector output) throws Exception {
        Optional<Delivery> deliveryOptional = handover.pollNext();
        if (deliveryOptional.isPresent()) {
            Delivery delivery = deliveryOptional.get();
            AMQP.BasicProperties properties = delivery.getProperties();
            byte[] body = delivery.getBody();
            Envelope envelope = delivery.getEnvelope();
            synchronized (output.getCheckpointLock()) {
                boolean newMessage = verifyMessageIdentifier(properties.getCorrelationId(), envelope.getDeliveryTag());
                if (!newMessage) {
                    return;
                }
                deliveryTagsProcessedForCurrentSnapshot.add(envelope.getDeliveryTag());
                deserializationSchema.deserialize(body, output);
            }

            if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
                // signal to the source that we have reached the end of the data.
                // rabbitmq source connector on support streaming mode, this is for test
                context.signalNoMoreElement();
            }
        }
    }

    @Override
    public List snapshotState(long checkpointId) throws Exception {

        List<RabbitmqSplit> pendingSplit = Collections.singletonList(new RabbitmqSplit(deliveryTagsProcessedForCurrentSnapshot, correlationIdsProcessedButNotAcknowledged));
        // perform a snapshot for these splits.
        List<Long> deliveryTags =
                pendingDeliveryTagsToCommit.computeIfAbsent(checkpointId, id -> new ArrayList<>());
        Set<String> correlationIds =
                pendingCorrelationIdsToCommit.computeIfAbsent(checkpointId, id -> new HashSet<>());
        // put currentCheckPoint deliveryTags and CorrelationIds.
        for (RabbitmqSplit split : pendingSplit) {
            List<Long> currentCheckPointDeliveryTags = split.getDeliveryTags();
            Set<String> currentCheckPointCorrelationIds = split.getCorrelationIds();

            if (currentCheckPointDeliveryTags != null) {
                deliveryTags.addAll(currentCheckPointDeliveryTags);
            }
            if (currentCheckPointCorrelationIds != null) {
                correlationIds.addAll(currentCheckPointCorrelationIds);
            }
        }
        return pendingSplit;
    }

    @Override
    public void addSplits(List splits) {
        //do nothing
    }

    @Override
    public void handleNoMoreSplits() {
        //do nothing
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        log.debug("Committing cursors for checkpoint {}", checkpointId);
        List<Long> pendingDeliveryTags = pendingDeliveryTagsToCommit.remove(checkpointId);
        Set<String> pendingCorrelationIds = pendingCorrelationIdsToCommit.remove(checkpointId);

        if (pendingDeliveryTags == null || pendingCorrelationIds == null) {
            log.debug(
                    "pending delivery tags or correlationIds checkpoint {} either do not exist or have already been committed.",
                    checkpointId);
            return;
        }
        acknowledgeDeliveryTags(pendingDeliveryTags);
        correlationIdsProcessedButNotAcknowledged.removeAll(pendingCorrelationIds);

    }

    protected void acknowledgeDeliveryTags(List<Long> deliveryTags) {
        try {
            for (long id : deliveryTags) {
                channel.basicAck(id, false);
            }
            channel.txCommit();
        } catch (IOException e) {
            throw new RabbitmqConnectorException(MESSAGE_ACK_FAILED, e);
        }
    }

    public boolean verifyMessageIdentifier(String correlationId, long deliveryTag) {
        if (!autoAck) {
            if (usesCorrelationId) {
                com.google.common.base.Preconditions.checkNotNull(
                        correlationId,
                        "RabbitMQ source was instantiated with usesCorrelationId set to "
                                + "true yet we couldn't extract the correlation id from it!");
                if (!correlationIdsProcessedButNotAcknowledged.add(correlationId)) {
                    // we have already processed this message
                    try {
                        channel.basicReject(deliveryTag, false);
                    } catch (IOException e) {
                        throw new RabbitmqConnectorException(MESSAGE_ACK_REJECTED, e);
                    }
                    return false;
                }
            }
        }
        return true;
    }
}
