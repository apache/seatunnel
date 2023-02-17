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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSemantics;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.ASSIGN_PARTITIONS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.DEFAULT_FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.DEFAULT_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.KAFKA_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.PARTITION;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.PARTITION_KEY_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.TOPIC;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.TRANSACTION_PREFIX;

/** KafkaSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Kafka. */
public class KafkaSinkWriter implements SinkWriter<SeaTunnelRow, KafkaCommitInfo, KafkaSinkState> {

    private final SinkWriter.Context context;

    private String transactionPrefix;
    private String topic;
    private long lastCheckpointId = 0;
    private boolean isExtractTopic;
    private SeaTunnelRowType seaTunnelRowType;

    private final KafkaProduceSender<byte[], byte[]> kafkaProducerSender;
    private final SeaTunnelRowSerializer<byte[], byte[]> seaTunnelRowSerializer;

    private static final int PREFIX_RANGE = 10000;

    public KafkaSinkWriter(
            SinkWriter.Context context,
            SeaTunnelRowType seaTunnelRowType,
            Config pluginConfig,
            List<KafkaSinkState> kafkaStates) {
        this.context = context;
        this.seaTunnelRowType = seaTunnelRowType;
        Pair<Boolean, String> topicResult = isExtractTopic(pluginConfig.getString(TOPIC.key()));
        this.isExtractTopic = topicResult.getKey();
        this.topic = topicResult.getRight();
        if (pluginConfig.hasPath(ASSIGN_PARTITIONS.key())) {
            MessageContentPartitioner.setAssignPartitions(
                    pluginConfig.getStringList(ASSIGN_PARTITIONS.key()));
        }
        if (pluginConfig.hasPath(TRANSACTION_PREFIX.key())) {
            this.transactionPrefix = pluginConfig.getString(TRANSACTION_PREFIX.key());
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
                seaTunnelRowSerializer.serializeRow(extractTopic(element), element);
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
                    CommonErrorCode.WRITER_OPERATION_FAILED, "Close kafka sink writer error", e);
        }
    }

    private Properties getKafkaProperties(Config pluginConfig) {
        Properties kafkaProperties = new Properties();
        if (CheckConfigUtil.isValidParam(pluginConfig, KAFKA_CONFIG.key())) {
            pluginConfig
                    .getObject(KAFKA_CONFIG.key())
                    .forEach((key, value) -> kafkaProperties.put(key, value.unwrapped()));
        }
        if (pluginConfig.hasPath(ASSIGN_PARTITIONS.key())) {
            kafkaProperties.put(
                    ProducerConfig.PARTITIONER_CLASS_CONFIG,
                    "org.apache.seatunnel.connectors.seatunnel.kafka.sink.MessageContentPartitioner");
        }
        kafkaProperties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                pluginConfig.getString(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        kafkaProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return kafkaProperties;
    }

    private SeaTunnelRowSerializer<byte[], byte[]> getSerializer(
            Config pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        String format = DEFAULT_FORMAT;
        if (pluginConfig.hasPath(FORMAT.key())) {
            format = pluginConfig.getString(FORMAT.key());
        }
        String delimiter = DEFAULT_FIELD_DELIMITER;
        if (pluginConfig.hasPath(FIELD_DELIMITER.key())) {
            delimiter = pluginConfig.getString(FIELD_DELIMITER.key());
        }
        if (pluginConfig.hasPath(PARTITION.key())) {
            return new DefaultSeaTunnelRowSerializer(
                    pluginConfig.getInt(PARTITION.key()), seaTunnelRowType, format, delimiter);
        } else {
            return new DefaultSeaTunnelRowSerializer(
                    getPartitionKeyFields(pluginConfig, seaTunnelRowType),
                    seaTunnelRowType,
                    format,
                    delimiter);
        }
    }

    private KafkaSemantics getKafkaSemantics(Config pluginConfig) {
        if (pluginConfig.hasPath("semantics")) {
            return pluginConfig.getEnum(KafkaSemantics.class, "semantics");
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
            Config pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        if (pluginConfig.hasPath(PARTITION_KEY_FIELDS.key())) {
            List<String> partitionKeyFields =
                    pluginConfig.getStringList(PARTITION_KEY_FIELDS.key());
            List<String> rowTypeFieldNames = Arrays.asList(seaTunnelRowType.getFieldNames());
            for (String partitionKeyField : partitionKeyFields) {
                if (!rowTypeFieldNames.contains(partitionKeyField)) {
                    throw new KafkaConnectorException(
                            CommonErrorCode.ILLEGAL_ARGUMENT,
                            String.format(
                                    "Partition key field not found: %s, rowType: %s",
                                    partitionKeyField, rowTypeFieldNames));
                }
            }
            return partitionKeyFields;
        }
        return Collections.emptyList();
    }

    private Pair<Boolean, String> isExtractTopic(String topicConfig) {
        String regex = "\\$\\{(.*?)\\}";
        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(topicConfig);
        if (matcher.find()) {
            return Pair.of(true, matcher.group(1));
        }
        return Pair.of(false, topicConfig);
    }

    private String extractTopic(SeaTunnelRow row) {
        if (!isExtractTopic) {
            return topic;
        }
        List<String> fieldNames = Arrays.asList(seaTunnelRowType.getFieldNames());
        if (!fieldNames.contains(topic)) {
            throw new KafkaConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    String.format("Field name { %s } is not found!", topic));
        }
        int topicFieldIndex = seaTunnelRowType.indexOf(topic);
        Object topicFieldValue = row.getField(topicFieldIndex);
        if (topicFieldValue == null) {
            throw new KafkaConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT, "The column value is empty!");
        }
        return topicFieldValue.toString();
    }
}
