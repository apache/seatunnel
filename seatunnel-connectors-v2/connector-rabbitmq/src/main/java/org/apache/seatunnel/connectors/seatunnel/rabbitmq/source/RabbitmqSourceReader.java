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

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.client.RabbitmqClient;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqBaseOptions;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.MESSAGE_ACK_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.MESSAGE_ACK_REJECTED;

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

    private final Map<String, DeserializationSchema<SeaTunnelRow>> schemaMap;
    private final Map<String, String> exactTableIdMap;
    private final Set<RabbitmqSplit> sourceSplits;
    private volatile boolean noMoreSplitsAssigned = false;

    public RabbitmqSourceReader(
            List<CatalogTable> catalogTables, SourceReader.Context context, RabbitmqConfig config) {
        this.queue = new LinkedBlockingQueue<>();
        this.pendingDeliveryTagsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.pendingCorrelationIdsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.context = context;
        this.config = config;
        this.rabbitMQClient = new RabbitmqClient(config);
        this.channel = rabbitMQClient.getChannel();
        this.usesCorrelationId = config.isUsesCorrelationId();
        this.sourceSplits = new HashSet<>();
        this.schemaMap = new HashMap<>();
        this.exactTableIdMap = new HashMap<>();

        // Initialize schema map for multi-table and single-table support
        for (CatalogTable table : catalogTables) {
            String queueName = table.getTableId().getTableName();

            if (queueName == null || "default".equalsIgnoreCase(queueName) || queueName.isEmpty()) {
                if (table.getOptions() != null
                        && table.getOptions().containsKey(RabbitmqBaseOptions.QUEUE_NAME.key())) {
                    queueName = table.getOptions().get(RabbitmqBaseOptions.QUEUE_NAME.key());
                }
            }

            if (queueName == null || "default".equalsIgnoreCase(queueName) || queueName.isEmpty()) {
                queueName = config.getQueueName();
            }

            if (queueName != null && !queueName.isEmpty()) {
                this.schemaMap.put(queueName, new JsonDeserializationSchema(table, false, false));
                this.exactTableIdMap.put(queueName, table.getTableId().toTablePath().toString());
            }
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
        DeliveryMessage message = queue.poll(5000, TimeUnit.MILLISECONDS);

        if (message != null) {
            Delivery delivery = message.getDelivery();
            if (delivery == null || delivery.getEnvelope() == null) {
                return;
            }

            AMQP.BasicProperties properties = delivery.getProperties();
            String correlationId = (properties != null) ? properties.getCorrelationId() : null;

            synchronized (output.getCheckpointLock()) {
                if (!verifyMessageIdentifier(
                        correlationId, delivery.getEnvelope().getDeliveryTag())) {
                    return;
                }
                deliveryTagsProcessedForCurrentSnapshot.add(
                        delivery.getEnvelope().getDeliveryTag());

                DeserializationSchema<SeaTunnelRow> schema = schemaMap.get(message.getSplitId());
                String exactTableId = exactTableIdMap.get(message.getSplitId());

                if (schema != null && exactTableId != null) {
                    SeaTunnelRow row = schema.deserialize(delivery.getBody());

                    if (row != null) {
                        row.setTableId(exactTableId);
                        output.collect(row);
                    }
                } else {
                    log.warn("Cannot find schema or tableId for queue: {}", message.getSplitId());
                }
            }
        }

        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            if (noMoreSplitsAssigned && queue.isEmpty()) {
                log.info("No more splits assigned and queue is empty. Signaling end of input.");
                context.signalNoMoreElement();
            }
        }
    }

    @Override
    public void addSplits(List<RabbitmqSplit> splits) {
        for (RabbitmqSplit split : splits) {
            System.out.println(
                    "\u001B[32m [READER DEBUG] Received split for queue: "
                            + split.splitId()
                            + "\u001B[0m");
            try {
                // For Bounded jobs (batch mode), signal end of input when the queue is drained.
                DefaultConsumer consumer =
                        rabbitMQClient.getQueueingConsumer(queue, split.splitId());

                channel.basicConsume(split.splitId(), autoAck, consumer);
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
