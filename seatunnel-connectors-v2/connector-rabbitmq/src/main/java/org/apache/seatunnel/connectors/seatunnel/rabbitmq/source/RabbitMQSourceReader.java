package org.apache.seatunnel.connectors.seatunnel.rabbitmq.source;

import com.google.common.base.Preconditions;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.common.Handover;
        import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitMQSplit;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class RabbitMQSourceReader implements SourceReader {
    protected final Handover<DeliveryWithSplitId> handover;
    protected final DeserializationSchema deserialization;

    private boolean noMoreSplitsAssignment = false;

    protected final SourceReader.Context context;
    protected final int batchSize;

    protected transient Connection connection;
    protected transient Channel channel;
    private final boolean usesCorrelationId;
    protected transient boolean autoAck;

    private transient volatile boolean running;

    protected transient List<Long> sessionIds;
    protected final Map<String, RabbitMQSplit> splitStates;
    protected final Set<String> finishedSplits;
    protected final SortedMap<Long, Map<String, List<Long>>> pendingCursorsToCommit;
    protected final Map<String, List<Long>> pendingCursorsToFinish;

    public RabbitMQSourceReader(Handover<Delivery> handover,
                                DeserializationSchema deserialization,
                                SourceReader.Context context,
                                int batchSize,
                                boolean usesCorrelationId) {
        this.handover = new Handover<>();
        this.deserialization = deserialization;
        this.pendingCursorsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.pendingCursorsToFinish = Collections.synchronizedSortedMap(new TreeMap<>());
        this.finishedSplits = new TreeSet<>();
        this.splitStates = new HashMap<>();
        this.context = context;
        this.batchSize = batchSize;
        this.usesCorrelationId = usesCorrelationId;
    }

    @Override
    public void open() throws Exception {
        sessionIds = new ArrayList<>(64);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void pollNext(Collector output) throws Exception {
        for (int i = 0; i < batchSize; i++) {
            Optional<DeliveryWithSplitId> deliveryOptional = handover.pollNext();
            if (deliveryOptional.isPresent()) {
                Delivery delivery = deliveryOptional.get().getDelivery();
                AMQP.BasicProperties properties = delivery.getProperties();
                byte[] body = delivery.getBody();
                Envelope envelope = delivery.getEnvelope();
                synchronized (output.getCheckpointLock()) {
                    splitStates.get(deliveryOptional.get().getSplitId()).getDeliveryTags().add(envelope.getDeliveryTag());
                    deserialization.deserialize(body, output);
                }
            }
            if (noMoreSplitsAssignment && finishedSplits.size() == splitStates.size()) {
                context.signalNoMoreElement();
                break;
            }
        }
    }

    @Override
    public List snapshotState(long checkpointId) throws Exception {
        List<RabbitMQSplit> pendingSplit = splitStates.values().stream()
                .map(RabbitMQSplit::copy)
                .collect(Collectors.toList());
        // Perform a snapshot for these splits.
        int size = pendingSplit.size();
        Map<String, List<Long>> cursors =
                pendingCursorsToCommit.computeIfAbsent(checkpointId, id -> new HashMap<>(size));
        // Put currentCheckPoint deliveryTags.
        for (RabbitMQSplit split : pendingSplit) {
            List<Long> currentCheckPointDeliveryTags = split.getDeliveryTags();
            if (currentCheckPointDeliveryTags != null) {
                cursors.put(split.splitId(), currentCheckPointDeliveryTags);
            }
        }
        return pendingSplit;
    }

    @Override
    public void addSplits(List splits) {

    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader received NoMoreSplits event.");
        this.noMoreSplitsAssignment = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        log.debug("Committing cursors for checkpoint {}", checkpointId);
        Map<String, List<Long>> pendingCursors = pendingCursorsToCommit.remove(checkpointId);
        if (pendingCursors == null) {
            log.debug(
                    "Cursors for checkpoint {} either do not exist or have already been committed.",
                    checkpointId);
            return;
        }
        pendingCursors.forEach((splitId, deliveryTags) -> {
            if (finishedSplits.contains(splitId)) {
                return;
            }
            acknowledgeSessionIDs(deliveryTags);

//            if (pendingCursorsToFinish.containsKey(splitId) &&
//                    pendingCursorsToFinish.get(splitId).compareTo(messageId) == 0) {
//                finishedSplits.add(splitId);
//                try {
//                    splitReaders.get(splitId).close();
//                } catch (IOException e) {
//                    throw new RuntimeException("Failed to close the split reader thread.", e);
//                }
//            }
        });

    }

    protected void acknowledgeSessionIDs(List<Long> sessionIds) {
        try {
            for (long id : sessionIds) {
                channel.basicAck(id, false);
            }
            channel.txCommit();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Messages could not be acknowledged during checkpoint creation.", e);
        }
    }
}
