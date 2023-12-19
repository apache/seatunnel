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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarClientConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConfigUtil;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSemantics;
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

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SinkProperties.DEFAULT_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SinkProperties.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SinkProperties.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SinkProperties.MESSAGE_ROUTING_MODE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SinkProperties.PARTITION_KEY_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SinkProperties.SEMANTICS;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SinkProperties.TEXT_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SinkProperties.TOPIC;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SinkProperties.TRANSACTION_TIMEOUT;

public class PulsarSinkWriter
        implements SinkWriter<SeaTunnelRow, PulsarCommitInfo, PulsarSinkState> {

    private Context context;
    private Producer<byte[]> producer;
    private PulsarClient pulsarClient;
    private SerializationSchema serializationSchema;
    private SerializationSchema keySerializationSchema;
    private TransactionImpl transaction;
    private int transactionTimeout = TRANSACTION_TIMEOUT.defaultValue();
    private PulsarSemantics pulsarSemantics = SEMANTICS.defaultValue();
    private final AtomicLong pendingMessages;

    public PulsarSinkWriter(
            Context context,
            PulsarClientConfig clientConfig,
            SeaTunnelRowType seaTunnelRowType,
            ReadonlyConfig pluginConfig,
            List<PulsarSinkState> pulsarStates) {
        this.context = context;
        String topic = pluginConfig.get(TOPIC);
        String format = pluginConfig.get(FORMAT);
        String delimiter = pluginConfig.get(FIELD_DELIMITER);
        Integer transactionTimeout = pluginConfig.get(TRANSACTION_TIMEOUT);
        PulsarSemantics pulsarSemantics = pluginConfig.get(SEMANTICS);
        MessageRoutingMode messageRoutingMode = pluginConfig.get(MESSAGE_ROUTING_MODE);
        this.serializationSchema = createSerializationSchema(seaTunnelRowType, format, delimiter);
        List<String> partitionKeyList = getPartitionKeyFields(pluginConfig, seaTunnelRowType);
        this.keySerializationSchema =
                createKeySerializationSchema(partitionKeyList, seaTunnelRowType);
        this.pulsarClient = PulsarConfigUtil.createClient(clientConfig, pulsarSemantics);

        if (PulsarSemantics.EXACTLY_ONCE == pulsarSemantics) {
            try {
                this.transaction =
                        (TransactionImpl)
                                PulsarConfigUtil.getTransaction(pulsarClient, transactionTimeout);
            } catch (Exception e) {
                throw new PulsarConnectorException(
                        PulsarConnectorErrorCode.CREATE_TRANSACTION_FAILED,
                        "Pulsar transaction create fail.");
            }
        }
        try {
            this.producer =
                    PulsarConfigUtil.createProducer(
                            pulsarClient, topic, pulsarSemantics, pluginConfig, messageRoutingMode);
        } catch (PulsarClientException e) {
            throw new PulsarConnectorException(
                    PulsarConnectorErrorCode.CREATE_PRODUCER_FAILED,
                    "Pulsar Producer create fail.");
        }
        this.pendingMessages = new AtomicLong(0);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        byte[] message = serializationSchema.serialize(element);
        byte[] key = null;
        if (keySerializationSchema != null) {
            key = keySerializationSchema.serialize(element);
        }
        TypedMessageBuilder<byte[]> typedMessageBuilder =
                PulsarConfigUtil.createTypedMessageBuilder(producer, transaction);
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
                            throw new PulsarConnectorException(
                                    PulsarConnectorErrorCode.SEND_MESSAGE_FAILED,
                                    "send message failed");
                        }
                    });
        }
    }

    @Override
    public Optional<PulsarCommitInfo> prepareCommit() throws IOException {
        if (PulsarSemantics.EXACTLY_ONCE == pulsarSemantics) {
            PulsarCommitInfo pulsarCommitInfo = new PulsarCommitInfo(this.transaction.getTxnID());
            return Optional.of(pulsarCommitInfo);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<PulsarSinkState> snapshotState(long checkpointId) throws IOException {
        if (PulsarSemantics.NON != pulsarSemantics) {
            /** flush pending messages */
            producer.flush();
            while (pendingMessages.longValue() > 0) {
                producer.flush();
            }
        }
        if (PulsarSemantics.EXACTLY_ONCE == pulsarSemantics) {
            List<PulsarSinkState> pulsarSinkStates =
                    Lists.newArrayList(new PulsarSinkState(this.transaction.getTxnID()));
            try {
                this.transaction =
                        (TransactionImpl)
                                PulsarConfigUtil.getTransaction(pulsarClient, transactionTimeout);
            } catch (Exception e) {
                throw new PulsarConnectorException(
                        PulsarConnectorErrorCode.CREATE_TRANSACTION_FAILED,
                        "Pulsar transaction create fail.");
            }
            return pulsarSinkStates;
        }
        return Collections.emptyList();
    }

    @Override
    public void abortPrepare() {
        if (PulsarSemantics.EXACTLY_ONCE == pulsarSemantics) {
            transaction.abort();
        }
    }

    @Override
    public void close() throws IOException {
        producer.close();
        pulsarClient.close();
    }

    private SerializationSchema createSerializationSchema(
            SeaTunnelRowType rowType, String format, String delimiter) {
        if (DEFAULT_FORMAT.equals(format)) {
            return new JsonSerializationSchema(rowType);
        } else if (TEXT_FORMAT.equals(format)) {
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
        if (pluginConfig.get(PARTITION_KEY_FIELDS) != null) {
            List<String> partitionKeyFields = pluginConfig.get(PARTITION_KEY_FIELDS);
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
}
