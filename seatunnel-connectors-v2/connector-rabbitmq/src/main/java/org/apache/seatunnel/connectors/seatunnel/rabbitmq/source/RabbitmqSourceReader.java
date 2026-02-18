package org.apache.seatunnel.connectors.seatunnel.rabbitmq.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.client.RabbitmqClient;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplit;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.MESSAGE_ACK_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.MESSAGE_ACK_REJECTED;

@Slf4j
public class RabbitmqSourceReader implements SourceReader<SeaTunnelRow, RabbitmqSplit> {

    protected final Handover<Delivery> handover;
    protected final SourceReader.Context context;
    protected transient Channel channel;
    private final boolean usesCorrelationId;
    protected transient boolean autoAck;

    protected transient Set<String> correlationIdsProcessedButNotAcknowledged;
    protected transient List<Long> deliveryTagsProcessedForCurrentSnapshot;

    protected final SortedMap<Long, List<Long>> pendingDeliveryTagsToCommit;
    protected final SortedMap<Long, Set<String>> pendingCorrelationIdsToCommit;

    private RabbitmqClient rabbitMQClient;
    private DefaultConsumer consumer;
    private final RabbitmqConfig config;

    private final Map<String, DeserializationSchema<SeaTunnelRow>> schemaMap;
    private final Map<String, String> queueToTableIdMap;
    private final Set<String> assignedQueues;

    public RabbitmqSourceReader(
            List<CatalogTable> catalogTables, SourceReader.Context context, RabbitmqConfig config) {
        this.handover = new Handover<>();
        this.pendingDeliveryTagsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.pendingCorrelationIdsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.context = context;
        this.config = config;
        this.rabbitMQClient = new RabbitmqClient(config);
        this.channel = rabbitMQClient.getChannel();
        this.usesCorrelationId = config.isUsesCorrelationId();
        this.schemaMap = new HashMap<>();
        this.queueToTableIdMap = new HashMap<>();
        this.assignedQueues = new HashSet<>();

        if (catalogTables != null) {
            for (CatalogTable table : catalogTables) {
                String queueName = table.getOptions().get(RabbitmqSourceOptions.QUEUE_NAME.key());

                if (queueName == null) {
                    queueName = config.getQueueName();
                }

                if (queueName != null) {
                    schemaMap.put(queueName, new JsonDeserializationSchema(table, false, false));
                    TablePath tablePath = table.getTablePath();
                    queueToTableIdMap.put(queueName, tablePath.toString());

                    log.info("Mapped Queue '{}' to TableID '{}'", queueName, tablePath.toString());
                }
            }
        }
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

        log.info("Starting RabbitMQ source reader (autoAck: {})", autoAck);
    }

    @Override
    public void close() throws IOException {
        if (rabbitMQClient != null) {
            rabbitMQClient.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        Optional<Delivery> deliveryOptional = handover.pollNext();
        if (deliveryOptional.isPresent()) {
            Delivery delivery = deliveryOptional.get();
            AMQP.BasicProperties properties = delivery.getProperties();
            byte[] body = delivery.getBody();
            Envelope envelope = delivery.getEnvelope();

            synchronized (output.getCheckpointLock()) {
                String correlationId = (properties != null) ? properties.getCorrelationId() : null;
                if (!verifyMessageIdentifier(correlationId, envelope.getDeliveryTag())) {
                    return;
                }
                deliveryTagsProcessedForCurrentSnapshot.add(envelope.getDeliveryTag());

                String sourceQueue = envelope.getRoutingKey();

                DeserializationSchema<SeaTunnelRow> currentSchema = schemaMap.get(sourceQueue);
                String correctTableId = queueToTableIdMap.get(sourceQueue);

                if (currentSchema == null && !schemaMap.isEmpty()) {
                    String defaultKey = schemaMap.keySet().iterator().next();
                    currentSchema = schemaMap.get(defaultKey);
                    correctTableId = queueToTableIdMap.get(defaultKey);
                }

                if (currentSchema != null && correctTableId != null) {
                    final String tableIdToUse = correctTableId;

                    currentSchema.deserialize(
                            body,
                            new Collector<SeaTunnelRow>() {
                                @Override
                                public void collect(SeaTunnelRow record) {
                                    record.setTableId(tableIdToUse);
                                    output.collect(record);
                                }

                                @Override
                                public Object getCheckpointLock() {
                                    return output.getCheckpointLock();
                                }
                            });
                } else {
                    log.warn("No schema or TableID found for routing key: {}", sourceQueue);
                }
            }
        }

        if (Boundedness.BOUNDED.equals(context.getBoundedness()) && handover.isEmpty()) {
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<RabbitmqSplit> snapshotState(long checkpointId) throws Exception {
        List<RabbitmqSplit> pendingSplit =
                Collections.singletonList(
                        new RabbitmqSplit(
                                "rabbitmq-split-" + checkpointId,
                                assignedQueues.isEmpty()
                                        ? "unassigned"
                                        : assignedQueues.iterator().next(),
                                new ArrayList<>(deliveryTagsProcessedForCurrentSnapshot),
                                new HashSet<>(correlationIdsProcessedButNotAcknowledged)));

        List<Long> deliveryTags =
                pendingDeliveryTagsToCommit.computeIfAbsent(checkpointId, id -> new ArrayList<>());
        Set<String> correlationIds =
                pendingCorrelationIdsToCommit.computeIfAbsent(checkpointId, id -> new HashSet<>());

        for (RabbitmqSplit split : pendingSplit) {
            if (split.getDeliveryTags() != null) deliveryTags.addAll(split.getDeliveryTags());
            if (split.getCorrelationIds() != null) correlationIds.addAll(split.getCorrelationIds());
        }
        deliveryTagsProcessedForCurrentSnapshot.clear();
        return pendingSplit;
    }

    @Override
    public void addSplits(List<RabbitmqSplit> splits) {
        for (RabbitmqSplit split : splits) {
            String queueName = split.getQueueName();
            if (queueName != null && !assignedQueues.contains(queueName)) {
                try {
                    log.info("Source Reader adding split, consuming from queue: {}", queueName);
                    channel.basicConsume(queueName, autoAck, consumer);
                    assignedQueues.add(queueName);
                } catch (IOException e) {
                    throw new RabbitmqConnectorException(
                            RabbitmqConnectorErrorCode.CREATE_RABBITMQ_CLIENT_FAILED,
                            "Failed to start consuming from queue: " + queueName,
                            e);
                }
            }
        }
    }

    @Override
    public void handleNoMoreSplits() {
        // do nothing
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        List<Long> pendingDeliveryTags = pendingDeliveryTagsToCommit.remove(checkpointId);
        Set<String> pendingCorrelationIds = pendingCorrelationIdsToCommit.remove(checkpointId);

        if (pendingDeliveryTags == null || pendingCorrelationIds == null) {
            return;
        }

        if (!autoAck) {
            acknowledgeDeliveryTags(pendingDeliveryTags);
        }
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
