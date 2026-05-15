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

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.client.RabbitmqClient;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplit;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.MESSAGE_ACK_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.MESSAGE_ACK_REJECTED;

/**
 * The reader implementation for RabbitMQ. Responsible for fetching messages from one or multiple
 * RabbitMQ queues, deserializing them using the correct schema, and passing them downstream.
 */
@Slf4j
public class RabbitmqSourceReader implements SourceReader<SeaTunnelRow, RabbitmqSplit> {
    private final BlockingQueue<DeliveryMessage> queue;
    protected final SourceReader.Context context;
    protected transient Channel channel;
    private final boolean usesCorrelationId;
    protected transient boolean autoAck;

    protected transient Set<String> correlationIdsProcessedButNotAcknowledged;
    protected transient List<Long> deliveryTagsProcessedForCurrentSnapshot;

    protected final SortedMap<Long, List<Long>> pendingDeliveryTagsToCommit;
    protected final SortedMap<Long, Set<String>> pendingCorrelationIdsToCommit;

    private RabbitmqClient rabbitMQClient;
    private final RabbitmqConfig config;

    // Maps used for Multi-Table routing.
    // They map the source queue name (split ID) to its specific deserialization schema and table
    // ID.
    private final Map<String, DeserializationSchema<SeaTunnelRow>> schemaMap;
    private final Map<String, String> exactTableIdMap;
    private final Set<RabbitmqSplit> sourceSplits;
    private final Map<String, DefaultConsumer> activeConsumers;
    private volatile boolean noMoreSplitsAssigned = false;

    /**
     * Constructor for RabbitmqSourceReader.
     *
     * @param queueToTableMap map of queue names to their corresponding CatalogTable
     * @param context source context
     * @param config rabbitmq config
     */
    public RabbitmqSourceReader(
            Map<String, CatalogTable> queueToTableMap,
            SourceReader.Context context,
            RabbitmqConfig config) {
        this.queue = new LinkedBlockingQueue<>();
        this.pendingDeliveryTagsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.pendingCorrelationIdsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.context = context;
        this.config = config;
        this.rabbitMQClient = new RabbitmqClient(config);
        this.channel = rabbitMQClient.getChannel();
        this.usesCorrelationId = config.isUsesCorrelationId();

        this.sourceSplits = ConcurrentHashMap.newKeySet();
        this.activeConsumers = new ConcurrentHashMap<>();

        this.schemaMap = new HashMap<>();
        this.exactTableIdMap = new HashMap<>();

        // Initialize schemas and table IDs for all configured queues.
        // This ensures the reader knows how to parse and route messages from any incoming split.
        for (Map.Entry<String, CatalogTable> entry : queueToTableMap.entrySet()) {
            String queueName = entry.getKey();
            CatalogTable table = entry.getValue();

            this.schemaMap.put(queueName, new JsonDeserializationSchema(table, false, false));
            this.exactTableIdMap.put(queueName, table.getTableId().toTablePath().toString());
        }
    }

    @Override
    public void open() throws Exception {
        this.correlationIdsProcessedButNotAcknowledged = new HashSet<>();
        this.deliveryTagsProcessedForCurrentSnapshot = new ArrayList<>();

        if (Boundedness.UNBOUNDED.equals(context.getBoundedness())) {
            autoAck = false;
            // enables transaction mode
            channel.txSelect();
        } else {
            autoAck = true;
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        // Poll a message from the internal buffer.
        // Messages are pushed here asynchronously by the RabbitMQ DefaultConsumers.
        DeliveryMessage message = queue.poll(5000, TimeUnit.MILLISECONDS);

        if (message != null) {
            Delivery delivery = message.getDelivery();
            if (delivery == null || delivery.getEnvelope() == null) {
                return;
            }

            AMQP.BasicProperties properties = delivery.getProperties();
            String correlationId = (properties != null) ? properties.getCorrelationId() : null;

            synchronized (output.getCheckpointLock()) {
                // Ensure the message wasn't already processed (idempotency check)
                if (!verifyMessageIdentifier(
                        correlationId, delivery.getEnvelope().getDeliveryTag())) {
                    return;
                }

                // Record the delivery tag for the current snapshot (to be acked later)
                deliveryTagsProcessedForCurrentSnapshot.add(
                        delivery.getEnvelope().getDeliveryTag());

                // Multi-Table Logic: Retrieve the correct schema and table ID based on the queue
                // name (split ID)
                DeserializationSchema<SeaTunnelRow> schema = schemaMap.get(message.getSplitId());
                String exactTableId = exactTableIdMap.get(message.getSplitId());

                if (schema != null && exactTableId != null) {
                    SeaTunnelRow row = schema.deserialize(delivery.getBody());

                    if (row != null) {
                        // Tag the row with its specific Table ID to ensure downstream sinks route
                        // it correctly
                        row.setTableId(exactTableId);
                        output.collect(row);
                    }
                } else {
                    String errorMsg =
                            String.format(
                                    "Cannot find schema or tableId for queue: %s. "
                                            + "This queue is not configured in tables_configs. "
                                            + "Available queues: %s",
                                    message.getSplitId(), schemaMap.keySet());
                    log.error(errorMsg);
                    throw new RabbitmqConnectorException(
                            SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED, errorMsg);
                }
            }
        }

        // Bounded mode logic: Stop the job if all splits have been consumed and the queue is empty
        if (Boundedness.BOUNDED.equals(context.getBoundedness()) && noMoreSplitsAssigned) {
            if (message == null && queue.isEmpty()) {
                log.info(
                        "No more splits assigned, queue is empty, and polling timed out. Signaling end of input.");
                context.signalNoMoreElement();
            }
        }
    }

    @Override
    public void addSplits(List<RabbitmqSplit> splits) {
        // Dynamically start consuming from newly assigned queues (splits)
        for (RabbitmqSplit split : splits) {
            log.info("Received split for queue: {}", split.splitId());
            try {
                if (activeConsumers.containsKey(split.splitId())) {
                    log.warn("Consumer for queue {} already exists, skipping", split.splitId());
                    continue;
                }

                // Create a new consumer that feeds messages into the shared internal 'queue'
                DefaultConsumer consumer =
                        rabbitMQClient.getQueueingConsumer(queue, split.splitId());
                rabbitMQClient.setupQueue(split.splitId());
                channel.basicConsume(split.splitId(), autoAck, consumer);
                activeConsumers.put(split.splitId(), consumer);
                sourceSplits.add(split);

                log.info("Started consuming from queue: {}", split.splitId());
            } catch (IOException e) {
                throw new RabbitmqConnectorException(
                        org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception
                                .RabbitmqConnectorErrorCode.CREATE_RABBITMQ_CLIENT_FAILED,
                        e);
            }
        }
    }

    @Override
    public List<RabbitmqSplit> snapshotState(long checkpointId) throws Exception {
        List<Long> deliveryTags =
                pendingDeliveryTagsToCommit.computeIfAbsent(checkpointId, id -> new ArrayList<>());
        Set<String> correlationIds =
                pendingCorrelationIdsToCommit.computeIfAbsent(checkpointId, id -> new HashSet<>());
        deliveryTags.addAll(deliveryTagsProcessedForCurrentSnapshot);
        correlationIds.addAll(correlationIdsProcessedButNotAcknowledged);
        deliveryTagsProcessedForCurrentSnapshot.clear();

        return new ArrayList<>(sourceSplits);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        List<Long> pendingDeliveryTags = pendingDeliveryTagsToCommit.remove(checkpointId);
        Set<String> pendingCorrelationIds = pendingCorrelationIdsToCommit.remove(checkpointId);

        if (pendingDeliveryTags != null && !autoAck) {
            acknowledgeDeliveryTags(pendingDeliveryTags);
        }
        if (pendingCorrelationIds != null) {
            correlationIdsProcessedButNotAcknowledged.removeAll(pendingCorrelationIds);
        }
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

    /**
     * Verify message identifier.
     *
     * @param correlationId correlation id
     * @param deliveryTag delivery tag
     * @return true if valid
     */
    public boolean verifyMessageIdentifier(String correlationId, long deliveryTag) {
        if (!autoAck && usesCorrelationId) {
            if (correlationId == null) {
                log.warn(
                        "CorrelationId is missing but required, rejecting message tag: {}",
                        deliveryTag);
                try {
                    channel.basicReject(deliveryTag, false);
                } catch (IOException e) {
                    throw new RabbitmqConnectorException(MESSAGE_ACK_REJECTED, e);
                }
                return false;
            }
            if (!correlationIdsProcessedButNotAcknowledged.add(correlationId)) {
                try {
                    channel.basicReject(deliveryTag, false);
                } catch (IOException e) {
                    throw new RabbitmqConnectorException(MESSAGE_ACK_REJECTED, e);
                }
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<String, DefaultConsumer> entry : activeConsumers.entrySet()) {
            try {
                if (channel != null && channel.isOpen()) {
                    channel.basicCancel(entry.getValue().getConsumerTag());
                }
            } catch (IOException e) {
                log.error("Failed to cancel consumer for queue {}", entry.getKey(), e);
            }
        }
        activeConsumers.clear();

        if (rabbitMQClient != null) {
            rabbitMQClient.close();
        }
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Received handleNoMoreSplits event from Enumerator.");
        this.noMoreSplitsAssigned = true;
    }
}
