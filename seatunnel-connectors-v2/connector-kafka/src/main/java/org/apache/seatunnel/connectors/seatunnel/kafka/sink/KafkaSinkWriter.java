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
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.ASSIGN_PARTITIONS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.BOOTSTRAP_SERVERS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.KAFKA_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.SEMANTICS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.TRANSACTION_PREFIX;

/** KafkaSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Kafka. */
public class KafkaSinkWriter implements SinkWriter<SeaTunnelRow, KafkaCommitInfo, KafkaSinkState> {

    private final SinkWriter.Context context;

    /** Job-level prefix persisted in every checkpoint for legacy restore compatibility. */
    private String transactionPrefix;

    /** Transaction namespace owned by this writer in the current execution. */
    private final String writerTransactionPrefix;

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

        if (pluginConfig.get(TRANSACTION_PREFIX) != null) {
            this.transactionPrefix = pluginConfig.get(TRANSACTION_PREFIX);
        } else {
            Random random = new Random();
            this.transactionPrefix = String.format("SeaTunnel%04d", random.nextInt(PREFIX_RANGE));
        }

        restoreState(kafkaStates);
        // Keep the checkpointed job prefix unchanged, but give each writer its own namespace.
        this.writerTransactionPrefix = this.transactionPrefix + "-" + context.getIndexOfSubtask();
        this.seaTunnelRowSerializer = KafkaSinkSerializer.create(pluginConfig, seaTunnelRowType);
        if (KafkaSemantics.EXACTLY_ONCE.equals(getKafkaSemantics(pluginConfig))) {
            KafkaTransactionSender<byte[], byte[]> transactionSender =
                    new KafkaTransactionSender<>(
                            this.transactionPrefix,
                            this.writerTransactionPrefix,
                            getKafkaProperties(pluginConfig));
            this.kafkaProducerSender = transactionSender;
            if (!kafkaStates.isEmpty()) {
                abortRestoredTransactions(transactionSender, kafkaStates);
            }
            this.kafkaProducerSender.beginTransaction(
                    generateTransactionId(this.writerTransactionPrefix, this.lastCheckpointId + 1));
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
                generateTransactionId(this.writerTransactionPrefix, this.lastCheckpointId + 1));
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
            kafkaProperties.put(
                    MessageContentPartitioner.ASSIGN_PARTITIONS_CONFIG,
                    pluginConfig.get(ASSIGN_PARTITIONS));
        }

        kafkaProperties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, pluginConfig.get(BOOTSTRAP_SERVERS));
        kafkaProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return kafkaProperties;
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
            this.lastCheckpointId =
                    states.stream().mapToLong(KafkaSinkState::getCheckpointId).max().orElse(0L);
            for (KafkaSinkState state : states) {
                if (!this.transactionPrefix.equals(state.getTransactionIdPrefix())) {
                    throw new IllegalStateException(
                            "Cannot restore Kafka sink states with different transaction prefixes");
                }
            }
        }
    }

    /**
     * Fences every unfinished transaction namespace assigned to this writer by the engine's state
     * redistribution. Each writer state is assigned to exactly one restoring subtask. Validating
     * the original writer index against the current modulo assignment prevents one live writer from
     * fencing a namespace owned by a sibling.
     */
    private void abortRestoredTransactions(
            KafkaTransactionSender<byte[], byte[]> transactionSender, List<KafkaSinkState> states) {
        Map<String, Long> nextCheckpointByPrefix = new LinkedHashMap<>();
        for (KafkaSinkState state : states) {
            String restoredPrefix = getRestoredTransactionIdPrefix(state);
            validateRestoredStateOwner(restoredPrefix);
            nextCheckpointByPrefix.merge(restoredPrefix, state.getCheckpointId() + 1, Math::min);
        }
        nextCheckpointByPrefix.forEach(transactionSender::abortTransaction);
    }

    private String getRestoredTransactionIdPrefix(KafkaSinkState state) {
        String transactionId = state.getTransactionId();
        int transactionSequenceSeparator =
                transactionId == null ? -1 : transactionId.lastIndexOf('-');
        if (transactionSequenceSeparator < 0
                || transactionSequenceSeparator == transactionId.length() - 1) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot restore Kafka transaction [%s] for checkpoint [%d]",
                            transactionId, state.getCheckpointId()));
        }
        try {
            Long.parseLong(transactionId.substring(transactionSequenceSeparator + 1));
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot restore Kafka transaction [%s] for checkpoint [%d]",
                            transactionId, state.getCheckpointId()),
                    e);
        }
        String restoredPrefix = transactionId.substring(0, transactionSequenceSeparator);
        if (!restoredPrefix.equals(this.transactionPrefix)
                && !restoredPrefix.startsWith(this.transactionPrefix + "-")) {
            throw new IllegalStateException(
                    String.format(
                            "Restored Kafka transaction [%s] does not belong to prefix [%s]",
                            transactionId, this.transactionPrefix));
        }
        return restoredPrefix;
    }

    private void validateRestoredStateOwner(String restoredPrefix) {
        if (restoredPrefix.equals(this.transactionPrefix)) {
            // Legacy checkpoints used one shared namespace and carry no writer index.
            return;
        }
        String writerIndexSuffix = restoredPrefix.substring(this.transactionPrefix.length() + 1);
        final int restoredWriterIndex;
        try {
            restoredWriterIndex = Integer.parseInt(writerIndexSuffix);
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot determine the writer owner of Kafka transaction prefix [%s]",
                            restoredPrefix),
                    e);
        }
        int parallelism = context.getNumberOfParallelSubtasks();
        int currentWriterIndex = context.getIndexOfSubtask();
        if (parallelism <= 0
                || restoredWriterIndex < 0
                || restoredWriterIndex % parallelism != currentWriterIndex) {
            throw new IllegalStateException(
                    String.format(
                            "Kafka transaction prefix [%s] was assigned to writer [%d] of [%d], "
                                    + "which does not own that restored state",
                            restoredPrefix, currentWriterIndex, parallelism));
        }
    }
}
