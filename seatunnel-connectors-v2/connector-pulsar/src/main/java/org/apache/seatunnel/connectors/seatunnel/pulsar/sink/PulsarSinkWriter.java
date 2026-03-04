package org.apache.seatunnel.connectors.seatunnel.pulsar.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarClientConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConfigUtil;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSemantics;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.state.PulsarCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.pulsar.state.PulsarSinkState;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PulsarSinkWriter
        implements SinkWriter<SeaTunnelRow, PulsarCommitInfo, PulsarSinkState> {

    private final PulsarClient pulsarClient;
    private final SerializationSchema serializationSchema;
    private final PulsarSemantics pulsarSemantics;
    private final int transactionTimeout;
    private final ReadonlyConfig pluginConfig;

    private final Map<String, Producer<byte[]>> producerMap = new ConcurrentHashMap<>();
    private final AtomicLong pendingMessages = new AtomicLong(0);

    // SINGLE transaction per checkpoint
    private Transaction currentTransaction;

    public PulsarSinkWriter(
            Context context,
            PulsarClientConfig clientConfig,
            SeaTunnelRowType seaTunnelRowType,
            ReadonlyConfig pluginConfig,
            List<PulsarSinkState> pulsarStates) {

        this.pluginConfig = pluginConfig;
        this.transactionTimeout = pluginConfig.get(PulsarSinkOptions.TRANSACTION_TIMEOUT);
        this.pulsarSemantics = pluginConfig.get(PulsarSinkOptions.SEMANTICS);

        this.serializationSchema =
                createSerializationSchema(
                        seaTunnelRowType,
                        pluginConfig.get(PulsarSinkOptions.FORMAT),
                        pluginConfig.get(PulsarSinkOptions.FIELD_DELIMITER));

        this.pulsarClient = PulsarConfigUtil.createClient(clientConfig, pulsarSemantics);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {

        String topic = resolveTopic(element);

        Producer<byte[]> producer = producerMap.computeIfAbsent(topic, this::createProducer);

        byte[] message = serializationSchema.serialize(element);

        TypedMessageBuilder<byte[]> builder =
                PulsarConfigUtil.createTypedMessageBuilder(
                        producer, (TransactionImpl) getOrCreateTransaction());

        builder.value(message);

        if (PulsarSemantics.NON == pulsarSemantics) {
            builder.sendAsync();
        } else {
            pendingMessages.incrementAndGet();
            CompletableFuture<MessageId> future = builder.sendAsync();
            future.whenComplete(
                    (id, ex) -> {
                        pendingMessages.decrementAndGet();
                        if (ex != null) {
                            throw new PulsarConnectorException(
                                    PulsarConnectorErrorCode.SEND_MESSAGE_FAILED,
                                    "Send message failed");
                        }
                    });
        }
    }

    private String resolveTopic(SeaTunnelRow row) {
        if (row.getTableId() != null) {
            return row.getTableId();
        }
        return pluginConfig.get(PulsarSinkOptions.TOPIC);
    }

    private Producer<byte[]> createProducer(String topic) {
        try {
            return PulsarConfigUtil.createProducer(
                    pulsarClient,
                    topic,
                    pulsarSemantics,
                    pluginConfig,
                    pluginConfig.get(PulsarSinkOptions.MESSAGE_ROUTING_MODE));
        } catch (Exception e) {
            throw new PulsarConnectorException(
                    PulsarConnectorErrorCode.CREATE_PRODUCER_FAILED,
                    "Failed to create producer for topic: " + topic);
        }
    }

    private Transaction getOrCreateTransaction() {
        if (PulsarSemantics.EXACTLY_ONCE != pulsarSemantics) {
            return null;
        }

        if (currentTransaction == null) {
            try {
                currentTransaction =
                        PulsarConfigUtil.getTransaction(pulsarClient, transactionTimeout);
            } catch (Exception e) {
                throw new PulsarConnectorException(
                        PulsarConnectorErrorCode.CREATE_TRANSACTION_FAILED,
                        "Transaction create failed");
            }
        }

        return currentTransaction;
    }

    @Override
    public Optional<PulsarCommitInfo> prepareCommit() throws IOException {

        if (PulsarSemantics.EXACTLY_ONCE != pulsarSemantics) {
            return Optional.empty();
        }

        while (pendingMessages.get() > 0) {
            Thread.yield();
        }

        if (currentTransaction == null) {
            return Optional.empty();
        }

        TxnID txnID = currentTransaction.getTxnID();
        currentTransaction = null;

        return Optional.of(new PulsarCommitInfo(txnID));
    }

    @Override
    public List<PulsarSinkState> snapshotState(long checkpointId) throws IOException {

        for (Producer<byte[]> producer : producerMap.values()) {
            producer.flush();
        }

        while (pendingMessages.get() > 0) {
            for (Producer<byte[]> producer : producerMap.values()) {
                producer.flush();
            }
        }

        return Collections.emptyList();
    }

    @Override
    public void abortPrepare() {

        if (PulsarSemantics.EXACTLY_ONCE != pulsarSemantics) {
            return;
        }

        if (currentTransaction != null) {
            try {
                currentTransaction.abort();
            } catch (Exception ignored) {
            }
            currentTransaction = null;
        }
    }

    @Override
    public void close() throws IOException {
        for (Producer<byte[]> producer : producerMap.values()) {
            producer.close();
        }
        pulsarClient.close();
    }

    private SerializationSchema createSerializationSchema(
            SeaTunnelRowType rowType, String format, String delimiter) {

        if (PulsarSinkOptions.DEFAULT_FORMAT.equals(format)) {
            return new JsonSerializationSchema(rowType);
        } else if (PulsarSinkOptions.TEXT_FORMAT.equals(format)) {
            return TextSerializationSchema.builder()
                    .seaTunnelRowType(rowType)
                    .delimiter(delimiter)
                    .build();
        } else {
            throw new RuntimeException("Unsupported format: " + format);
        }
    }
}
