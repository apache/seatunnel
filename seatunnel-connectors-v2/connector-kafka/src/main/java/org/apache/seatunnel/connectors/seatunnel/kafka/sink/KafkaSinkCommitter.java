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
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Properties;

@Slf4j
public class KafkaSinkCommitter implements SinkCommitter<KafkaCommitInfo> {

    private final ReadonlyConfig pluginConfig;

    private KafkaInternalProducer<?, ?> kafkaProducer;

    public KafkaSinkCommitter(ReadonlyConfig pluginConfig) {
        this.pluginConfig = pluginConfig;
    }

    @Override
    public List<KafkaCommitInfo> commit(List<KafkaCommitInfo> commitInfos) {
        if (commitInfos.isEmpty()) {
            return commitInfos;
        }
        for (KafkaCommitInfo commitInfo : commitInfos) {
            String transactionId = commitInfo.getTransactionId();
            if (log.isDebugEnabled()) {
                log.debug("Committing transaction {}, commitInfo {}", transactionId, commitInfo);
            }
            KafkaProducer<?, ?> producer = getProducer(commitInfo);
            producer.commitTransaction();
            producer.flush();
        }
        if (this.kafkaProducer != null) {
            kafkaProducer.close();
            kafkaProducer = null;
        }
        return commitInfos;
    }

    @Override
    public void abort(List<KafkaCommitInfo> commitInfos) {
        if (commitInfos.isEmpty()) {
            return;
        }
        for (KafkaCommitInfo commitInfo : commitInfos) {
            KafkaProducer<?, ?> producer = getProducer(commitInfo);
            producer.abortTransaction();
        }
        if (this.kafkaProducer != null) {
            kafkaProducer.close();
            kafkaProducer = null;
        }
    }

    private KafkaInternalProducer<?, ?> getProducer(KafkaCommitInfo commitInfo) {
        if (this.kafkaProducer != null) {
            this.kafkaProducer.setTransactionalId(commitInfo.getTransactionId());
        } else {
            Properties kafkaProperties = commitInfo.getKafkaProperties();
            kafkaProperties.setProperty(
                    ProducerConfig.TRANSACTIONAL_ID_CONFIG, commitInfo.getTransactionId());
            kafkaProducer =
                    new KafkaInternalProducer<>(
                            commitInfo.getKafkaProperties(), commitInfo.getTransactionId());
        }
        kafkaProducer.resumeTransaction(
                commitInfo.getProducerId(), commitInfo.getEpoch(), commitInfo.isTxnStarted());
        return kafkaProducer;
    }
}
