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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSemantics;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.ASSIGN_PARTITIONS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.BOOTSTRAP_SERVERS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.DEFAULT_FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.KAFKA_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.PARTITION;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.PARTITION_KEY_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.SEMANTICS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.TOPIC;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.TRANSACTION_PREFIX;

/** KafkaSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Kafka. */
public class KafkaSinkWriter implements SinkWriter<SeaTunnelRow, KafkaCommitInfo, KafkaSinkState> {

    private final SinkWriter.Context context;

    private String transactionPrefix;
    private long lastCheckpointId = 0;
    private SeaTunnelRowType seaTunnelRowType;

    private final KafkaProduceSender<byte[], byte[]> kafkaProducerSender;
    private final SeaTunnelRowSerializer<byte[], byte[]> seaTunnelRowSerializer;

    private static final int PREFIX_RANGE = 10000;

    public KafkaSinkWriter(
            SinkWriter.Context context,
            SeaTunnelRowType seaTunnelRowType,
            ReadonlyConfig pluginConfig,
            List<KafkaSinkState> kafkaStates) {
        this.context = context;
        this.seaTunnelRowType = seaTunnelRowType;
        if (pluginConfig.get(ASSIGN_PARTITIONS) != null
                && !CollectionUtils.isEmpty(pluginConfig.get(ASSIGN_PARTITIONS))) {
            MessageContentPartitioner.setAssignPartitions(pluginConfig.get(ASSIGN_PARTITIONS));
        }

        if (pluginConfig.get(TRANSACTION_PREFIX) != null) {
            this.transactionPrefix = pluginConfig.get(TRANSACTION_PREFIX);
        } else {
            Random random = new Random();
            this.transactionPrefix = String.format("SeaTunnel%04d", random.nextInt(PREFIX_RANGE));
        }

        restoreState(kafkaStates);
        this.seaTunnelRowSerializer = getSerializer(pluginConfig, seaTunnelRowType);
        if (KafkaSemantics.EXACTLY_ONCE.equals(getKafkaSemantics(pluginConfig))) {
            this.kafkaProducerSender =
                    new KafkaTransactionSender<>(
                            this.transactionPrefix, getKafkaProperties(pluginConfig));
            // abort all transaction number bigger than current transaction, because they maybe
            // already start
            //  transaction.
            if (!kafkaStates.isEmpty()) {
                this.kafkaProducerSender.abortTransaction(kafkaStates.get(0).getCheckpointId() + 1);
            }
            this.kafkaProducerSender.beginTransaction(
                    generateTransactionId(this.transactionPrefix, this.lastCheckpointId + 1));
        } else {
            this.kafkaProducerSender =
                    new KafkaNoTransactionSender<>(getKafkaProperties(pluginConfig));
        }
    }

    @Override
    public void write(SeaTunnelRow element) {
        ProducerRecord<byte[], byte[]> producerRecord =
                seaTunnelRowSerializer.serializeRow(element);
        kafkaProducerSender.send(producerRecord);
    }

    @Override
    public List<KafkaSinkState> snapshotState(long checkpointId) {
        List<KafkaSinkState> states = kafkaProducerSender.snapshotState(checkpointId);
        this.lastCheckpointId = checkpointId;
        this.kafkaProducerSender.beginTransaction(
                generateTransactionId(this.transactionPrefix, this.lastCheckpointId + 1));
        return states;
    }

    @Override
    public Optional<KafkaCommitInfo> prepareCommit() {
        return kafkaProducerSender.prepareCommit();
    }

    @Override
    public void abortPrepare() {
        kafkaProducerSender.abortTransaction();
    }

    @Override
    public void close() {
        try {
            kafkaProducerSender.close();
        } catch (Exception e) {
            throw new KafkaConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "Close kafka sink writer error",
                    e);
        }
    }

    private Properties getKafkaProperties(ReadonlyConfig pluginConfig) {
        Properties kafkaProperties = new Properties();
        if (pluginConfig.get(KAFKA_CONFIG) != null) {
            pluginConfig.get(KAFKA_CONFIG).forEach((key, value) -> kafkaProperties.put(key, value));
        }

        if (pluginConfig.get(ASSIGN_PARTITIONS) != null) {
            kafkaProperties.put(
                    ProducerConfig.PARTITIONER_CLASS_CONFIG,
                    "org.apache.seatunnel.connectors.seatunnel.kafka.sink.MessageContentPartitioner");
        }

        kafkaProperties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, pluginConfig.get(BOOTSTRAP_SERVERS));
        kafkaProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return kafkaProperties;
    }

    private SeaTunnelRowSerializer<byte[], byte[]> getSerializer(
            ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        MessageFormat messageFormat = pluginConfig.get(FORMAT);
        String delimiter = DEFAULT_FIELD_DELIMITER;

        if (pluginConfig.get(FIELD_DELIMITER) != null) {
            delimiter = pluginConfig.get(FIELD_DELIMITER);
        }

        String topic = pluginConfig.get(TOPIC);
        if (pluginConfig.get(PARTITION_KEY_FIELDS) != null && pluginConfig.get(PARTITION) != null) {
            throw new KafkaConnectorException(
                    KafkaConnectorErrorCode.GET_TRANSACTIONMANAGER_FAILED,
                    "Cannot select both `partiton` and `partition_key_fields`. You can configure only one of them");
        }
        if (pluginConfig.get(PARTITION_KEY_FIELDS) != null) {
            return DefaultSeaTunnelRowSerializer.create(
                    topic,
                    getPartitionKeyFields(pluginConfig, seaTunnelRowType),
                    seaTunnelRowType,
                    messageFormat,
                    delimiter,
                    pluginConfig);
        }
        if (pluginConfig.get(PARTITION) != null) {
            return DefaultSeaTunnelRowSerializer.create(
                    topic,
                    pluginConfig.get(PARTITION),
                    seaTunnelRowType,
                    messageFormat,
                    delimiter,
                    pluginConfig);
        }
        // By default, all partitions are sent randomly
        return DefaultSeaTunnelRowSerializer.create(
                topic, Arrays.asList(), seaTunnelRowType, messageFormat, delimiter, pluginConfig);
    }

    private KafkaSemantics getKafkaSemantics(ReadonlyConfig pluginConfig) {
        if (pluginConfig.get(SEMANTICS) != null) {
            return pluginConfig.get(SEMANTICS);
        }
        return KafkaSemantics.NON;
    }

    protected static String generateTransactionId(String transactionPrefix, long checkpointId) {
        return transactionPrefix + "-" + checkpointId;
    }

    private void restoreState(List<KafkaSinkState> states) {
        if (!states.isEmpty()) {
            this.transactionPrefix = states.get(0).getTransactionIdPrefix();
            this.lastCheckpointId = states.get(0).getCheckpointId();
        }
    }

    private List<String> getPartitionKeyFields(
            ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType) {

        if (pluginConfig.get(PARTITION_KEY_FIELDS) != null) {
            List<String> partitionKeyFields = pluginConfig.get(PARTITION_KEY_FIELDS);
            List<String> rowTypeFieldNames = Arrays.asList(seaTunnelRowType.getFieldNames());
            for (String partitionKeyField : partitionKeyFields) {
                if (!rowTypeFieldNames.contains(partitionKeyField)) {
                    throw new KafkaConnectorException(
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
