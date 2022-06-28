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

import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Optional;

public interface KafkaProduceSender<K, V> extends AutoCloseable {
    /**
     * Send data to kafka.
     *
     * @param producerRecord data to send
     */
    void send(ProducerRecord<K, V> producerRecord);

    void beginTransaction(String transactionId);

    /**
     * Prepare a transaction commit.
     *
     * @return commit info, or empty if no commit is needed.
     */
    Optional<KafkaCommitInfo> prepareCommit();

    /**
     * Abort the current transaction.
     */
    void abortTransaction();

    /**
     * Abort the given transaction.
     *
     * @param checkpointId the id of the last checkpoint of the last run
     */
    void abortTransaction(long checkpointId);

    /**
     * Get the current kafka state of the sender.
     *
     * @return kafka state List, or empty if no state is available.
     */
    List<KafkaSinkState> snapshotState(long checkpointId);

}
