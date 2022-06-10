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

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.TRANSACTION_PREFIX;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSemantics;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;

/**
 * KafkaSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Kafka.
 */
public class KafkaSinkWriter implements SinkWriter<SeaTunnelRow, KafkaCommitInfo, KafkaSinkState> {

    private final SinkWriter.Context context;
    private final Config pluginConfig;

    private String transactionPrefix;
    private long lastCheckpointId = 0;

    private final KafkaProduceSender<String, String> kafkaProducerSender;

    private static final int PREFIX_RANGE = 10000;

    // check config
    @Override
    public void write(SeaTunnelRow element) {
        ProducerRecord<String, String> producerRecord = seaTunnelRowSerializer.serializeRow(element);
        kafkaProducerSender.send(producerRecord);
    }

    private final SeaTunnelRowSerializer<String, String> seaTunnelRowSerializer;

    public KafkaSinkWriter(
            SinkWriter.Context context,
            SeaTunnelRowTypeInfo seaTunnelRowTypeInfo,
            Config pluginConfig,
            List<KafkaSinkState> kafkaStates) {
        this.context = context;
        this.pluginConfig = pluginConfig;
        if (pluginConfig.hasPath(TRANSACTION_PREFIX)) {
            this.transactionPrefix = pluginConfig.getString(TRANSACTION_PREFIX);
        } else {
            Random random = new Random();
            this.transactionPrefix = String.format("SeaTunnel%04d", random.nextInt(PREFIX_RANGE));
        }
        restoreState(kafkaStates);
        this.seaTunnelRowSerializer = getSerializer(pluginConfig, seaTunnelRowTypeInfo);
        if (KafkaSemantics.EXACTLY_ONCE.equals(getKafkaSemantics(pluginConfig))) {
            // the recover state
            this.kafkaProducerSender =
                    new KafkaTransactionSender<>(this.transactionPrefix, getKafkaProperties(pluginConfig));
            // TODO abort all transaction number bigger than current transaction, cause they will commit
            //  transaction again.
            this.kafkaProducerSender.abortTransaction(kafkaStates);
            this.kafkaProducerSender.beginTransaction(generateTransactionId(this.transactionPrefix,
                    this.lastCheckpointId + 1));
        } else {
            this.kafkaProducerSender = new KafkaNoTransactionSender<>(getKafkaProperties(pluginConfig));
        }
    }

    @Override
    public List<KafkaSinkState> snapshotState(long checkpointId) {
        List<KafkaSinkState> states = kafkaProducerSender.snapshotState(checkpointId);
        this.lastCheckpointId = checkpointId;
        this.kafkaProducerSender.beginTransaction(generateTransactionId(this.transactionPrefix,
                this.lastCheckpointId + 1));
        return states;
    }

    @Override
    public Optional<KafkaCommitInfo> prepareCommit() {
        return kafkaProducerSender.prepareCommit();
    }

    @Override
    public void abort() {
        kafkaProducerSender.abortTransaction();
    }

    @Override
    public void close() {
        try (KafkaProduceSender<?, ?> kafkaProduceSender = kafkaProducerSender) {
            // no-opt
        } catch (Exception e) {
            throw new RuntimeException("Close kafka sink writer error", e);
        }
    }

    private Properties getKafkaProperties(Config pluginConfig) {
        Config kafkaConfig = TypesafeConfigUtils.extractSubConfig(pluginConfig,
                org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.KAFKA_CONFIG_PREFIX, true);
        Properties kafkaProperties = new Properties();
        kafkaConfig.entrySet().forEach(entry -> {
            kafkaProperties.put(entry.getKey(), entry.getValue().unwrapped());
        });
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, pluginConfig.getString(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return kafkaProperties;
    }

    // todo: parse the target field from config
    private SeaTunnelRowSerializer<String, String> getSerializer(Config pluginConfig, SeaTunnelRowTypeInfo seaTunnelRowTypeInfo) {
        return new DefaultSeaTunnelRowSerializer(pluginConfig.getString("topics"), seaTunnelRowTypeInfo);
    }

    private KafkaSemantics getKafkaSemantics(Config pluginConfig) {
        if (pluginConfig.hasPath("semantics")) {
            return pluginConfig.getEnum(KafkaSemantics.class, "semantics");
        }
        return KafkaSemantics.NON;
    }

    private String generateTransactionId(String transactionPrefix, long checkpointId) {
        return transactionPrefix + "-" + checkpointId;
    }

    private void restoreState(List<KafkaSinkState> states) {
        if (!states.isEmpty()) {
            this.transactionPrefix = states.get(0).getTransactionIdPrefix();
            this.lastCheckpointId = states.get(0).getCheckpointId();
        }
    }

}
