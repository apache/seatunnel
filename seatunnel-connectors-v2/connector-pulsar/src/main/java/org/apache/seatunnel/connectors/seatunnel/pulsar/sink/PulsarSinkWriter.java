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

package org.apache.seatunnel.connectors.seatunnel.pulsar.sink;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarClientConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConfigUtil;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSemantics;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.state.PulsarCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.pulsar.state.PulsarSinkState;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@Slf4j
public class PulsarSinkWriter
        implements SinkWriter<SeaTunnelRow, PulsarCommitInfo, PulsarSinkState>,
                SupportMultiTableSinkWriter<Void> {

    @FunctionalInterface
    interface ProducerCreator {
        Producer<byte[]> create(String topic) throws PulsarClientException;
    }

    private final Map<String, Producer<byte[]>> producerMap = new ConcurrentHashMap<>();
    private final AtomicLong pendingMessages;
    private final AtomicReference<Throwable> sendMessageException;
    private final ReadonlyConfig pluginConfig;
    private final MessageRoutingMode messageRoutingMode;
    private final ProducerCreator producerCreator;
    private PulsarClient pulsarClient;
    private SerializationSchema serializationSchema;
    private SerializationSchema keySerializationSchema;
    private volatile TransactionImpl transaction;
    private int transactionTimeout;
    private PulsarSemantics pulsarSemantics;

    public PulsarSinkWriter(
            Context context,
            PulsarClientConfig clientConfig,
            SeaTunnelRowType seaTunnelRowType,
            ReadonlyConfig pluginConfig,
            List<PulsarSinkState> pulsarStates) {
        this(
                seaTunnelRowType,
                pluginConfig,
                pulsarStates,
                PulsarConfigUtil.createClient(
                        clientConfig, pluginConfig.get(PulsarSinkOptions.SEMANTICS)),
                null);
    }

    PulsarSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            ReadonlyConfig pluginConfig,
            List<PulsarSinkState> pulsarStates,
            PulsarClient pulsarClient,
            ProducerCreator producerCreator) {
        String format = pluginConfig.get(PulsarSinkOptions.FORMAT);
        String delimiter = pluginConfig.get(PulsarSinkOptions.FIELD_DELIMITER);
        this.transactionTimeout = pluginConfig.get(PulsarSinkOptions.TRANSACTION_TIMEOUT);
        this.pulsarSemantics = pluginConfig.get(PulsarSinkOptions.SEMANTICS);
        this.messageRoutingMode = pluginConfig.get(PulsarSinkOptions.MESSAGE_ROUTING_MODE);
        this.serializationSchema = createSerializationSchema(seaTunnelRowType, format, delimiter);
        List<String> partitionKeyList = getPartitionKeyFields(pluginConfig, seaTunnelRowType);
        this.keySerializationSchema =
                createKeySerializationSchema(partitionKeyList, seaTunnelRowType);
        this.pulsarClient = pulsarClient;
        this.pluginConfig = pluginConfig;
        this.producerCreator =
                producerCreator != null
                        ? producerCreator
                        : topic ->
                                PulsarConfigUtil.createProducer(
                                        this.pulsarClient,
                                        topic,
                                        pulsarSemantics,
                                        pluginConfig,
                                        messageRoutingMode);
        this.pendingMessages = new AtomicLong(0);
        this.sendMessageException = new AtomicReference<>();

        if (PulsarSemantics.EXACTLY_ONCE == pulsarSemantics) {
            this.transaction = createTransaction();
        }
    }

    String resolveTopic(SeaTunnelRow row) {
        String tableId = row.getTableId();
        if (tableId != null && !tableId.isEmpty()) {
            return tableId;
        }

        String topic = pluginConfig.get(PulsarSinkOptions.TOPIC);
        if (topic == null || topic.isEmpty()) {
            throw new PulsarConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    "Topic must be configured when SeaTunnelRow.getTableId() is null");
        }

        return topic;
    }

    Producer<byte[]> getOrCreateProducer(String topic) {
        Producer<byte[]> existing = producerMap.get(topic);
        if (existing != null) {
            return existing;
        }

        try {
            Producer<byte[]> producer = producerCreator.create(topic);

            producerMap.put(topic, producer);
            return producer;

        } catch (PulsarClientException e) {
            throw new PulsarConnectorException(
                    PulsarConnectorErrorCode.CREATE_PRODUCER_FAILED,
                    "Failed to create Pulsar producer for topic: " + topic,
                    e);
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        checkSendException();

        String topic = resolveTopic(element);
        byte[] message = serializationSchema.serialize(element);
        byte[] key = null;
        if (keySerializationSchema != null) {
            key = keySerializationSchema.serialize(element);
        }

        Producer<byte[]> topicProducer = getOrCreateProducer(topic);

        TypedMessageBuilder<byte[]> typedMessageBuilder =
                PulsarConfigUtil.createTypedMessageBuilder(
                        topicProducer,
                        PulsarSemantics.EXACTLY_ONCE == pulsarSemantics ? transaction : null);

        if (key != null) {
            typedMessageBuilder.keyBytes(key);
        }
        typedMessageBuilder.value(message);
        if (PulsarSemantics.NON == pulsarSemantics) {
            typedMessageBuilder.sendAsync();
        } else {
            pendingMessages.incrementAndGet();
            CompletableFuture<MessageId> future = typedMessageBuilder.sendAsync();
            future.whenComplete(
                    (id, ex) -> {
                        pendingMessages.decrementAndGet();
                        if (ex != null) {
                            log.error("Failed to send message to topic {}", topic, ex);
                            sendMessageException.compareAndSet(null, ex);
                        }
                    });
        }
    }

    @Override
    public Optional<PulsarCommitInfo> prepareCommit() throws IOException {
        checkSendException();
        if (PulsarSemantics.EXACTLY_ONCE == pulsarSemantics && transaction != null) {
            PulsarCommitInfo pulsarCommitInfo = new PulsarCommitInfo(this.transaction.getTxnID());
            return Optional.of(pulsarCommitInfo);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<PulsarSinkState> snapshotState(long checkpointId) throws IOException {
        if (PulsarSemantics.NON != pulsarSemantics) {
            flushPendingMessages();
            checkSendException();
        }
        if (PulsarSemantics.EXACTLY_ONCE == pulsarSemantics) {
            List<PulsarSinkState> pulsarSinkStates =
                    Lists.newArrayList(new PulsarSinkState(this.transaction.getTxnID()));
            this.transaction = createTransaction();
            return pulsarSinkStates;
        }

        return Collections.emptyList();
    }

    @Override
    public void abortPrepare() {
        if (PulsarSemantics.EXACTLY_ONCE == pulsarSemantics && transaction != null) {
            transaction.abort();
        }
    }

    @Override
    public void close() throws IOException {
        Throwable closeFailure = null;
        for (Producer<byte[]> producer : producerMap.values()) {
            try {
                producer.close();
            } catch (Throwable throwable) {
                closeFailure = appendSuppressed(closeFailure, throwable);
            }
        }
        if (pulsarClient != null) {
            try {
                pulsarClient.close();
            } catch (Throwable throwable) {
                closeFailure = appendSuppressed(closeFailure, throwable);
            }
        }

        Throwable sendFailure = sendMessageException.get();
        if (sendFailure != null) {
            closeFailure = appendSuppressed(closeFailure, buildSendFailureException(sendFailure));
        }

        if (closeFailure != null) {
            rethrowCloseFailure(closeFailure);
        }
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
            throw new SeaTunnelJsonFormatException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported format: " + format);
        }
    }

    public static SerializationSchema createKeySerializationSchema(
            List<String> keyFieldNames, SeaTunnelRowType seaTunnelRowType) {
        if (keyFieldNames == null || keyFieldNames.isEmpty()) {
            return null;
        }
        int[] keyFieldIndexArr = new int[keyFieldNames.size()];
        SeaTunnelDataType[] keyFieldDataTypeArr = new SeaTunnelDataType[keyFieldNames.size()];
        for (int i = 0; i < keyFieldNames.size(); i++) {
            String keyFieldName = keyFieldNames.get(i);
            int rowFieldIndex = seaTunnelRowType.indexOf(keyFieldName);
            keyFieldIndexArr[i] = rowFieldIndex;
            keyFieldDataTypeArr[i] = seaTunnelRowType.getFieldType(rowFieldIndex);
        }
        SeaTunnelRowType keyType =
                new SeaTunnelRowType(keyFieldNames.toArray(new String[0]), keyFieldDataTypeArr);
        SerializationSchema keySerializationSchema = new JsonSerializationSchema(keyType);

        Function<SeaTunnelRow, SeaTunnelRow> keyDataExtractor =
                row -> {
                    Object[] keyFields = new Object[keyFieldIndexArr.length];
                    for (int i = 0; i < keyFieldIndexArr.length; i++) {
                        keyFields[i] = row.getField(keyFieldIndexArr[i]);
                    }
                    return new SeaTunnelRow(keyFields);
                };
        return row -> keySerializationSchema.serialize(keyDataExtractor.apply(row));
    }

    private List<String> getPartitionKeyFields(
            ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        if (pluginConfig.getOptional(PulsarSinkOptions.PARTITION_KEY_FIELDS).isPresent()) {
            List<String> partitionKeyFields =
                    pluginConfig.get(PulsarSinkOptions.PARTITION_KEY_FIELDS);
            List<String> rowTypeFieldNames = Arrays.asList(seaTunnelRowType.getFieldNames());
            for (String partitionKeyField : partitionKeyFields) {
                if (!rowTypeFieldNames.contains(partitionKeyField)) {
                    throw new PulsarConnectorException(
                            CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                            String.format(
                                    "Partition key field not found: %s, rowType: %s",
                                    partitionKeyField, rowTypeFieldNames));
                }
            }
            return partitionKeyFields;
        }
        return Collections.emptyList();
    }

    private TransactionImpl createTransaction() {
        try {
            return (TransactionImpl)
                    PulsarConfigUtil.getTransaction(pulsarClient, transactionTimeout);
        } catch (Exception e) {
            throw new PulsarConnectorException(
                    PulsarConnectorErrorCode.CREATE_TRANSACTION_FAILED,
                    "Pulsar transaction create fail.",
                    e);
        }
    }

    private void flushPendingMessages() throws IOException {
        for (Producer<byte[]> producer : producerMap.values()) {
            producer.flush();
        }

        while (pendingMessages.longValue() > 0) {
            checkSendException();
            for (Producer<byte[]> producer : producerMap.values()) {
                producer.flush();
            }
        }
    }

    private void checkSendException() {
        Throwable throwable = sendMessageException.get();
        if (throwable != null) {
            throw buildSendFailureException(throwable);
        }
    }

    private PulsarConnectorException buildSendFailureException(Throwable throwable) {
        return new PulsarConnectorException(
                PulsarConnectorErrorCode.SEND_MESSAGE_FAILED,
                "Send message failed, please check previous error log for details.",
                throwable);
    }

    private Throwable appendSuppressed(Throwable existingFailure, Throwable newFailure) {
        if (existingFailure == null) {
            return newFailure;
        }
        existingFailure.addSuppressed(newFailure);
        return existingFailure;
    }

    private void rethrowCloseFailure(Throwable throwable) throws IOException {
        if (throwable instanceof IOException) {
            throw (IOException) throwable;
        }
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        }
        throw new IOException("Failed to close Pulsar sink writer.", throwable);
    }

    @Override
    public MultiTableResourceManager<Void> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        return null;
    }

    @Override
    public void setMultiTableResourceManager(
            MultiTableResourceManager<Void> multiTableResourceManager, int queueIndex) {
        // Pulsar sink does not require shared resources across tables
    }
}
