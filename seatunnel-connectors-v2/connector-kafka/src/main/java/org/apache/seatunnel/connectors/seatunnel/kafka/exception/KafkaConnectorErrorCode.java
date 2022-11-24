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

package org.apache.seatunnel.connectors.seatunnel.kafka.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum KafkaConnectorErrorCode implements SeaTunnelErrorCode {
    VERSION_INCOMPATIBLE("Kafka-01", "Incompatible KafkaProducer version"),
    GET_TRANSACTIONMANAGER_FAILED("Kafka-02", "Get transactionManager in KafkaProducer failed"),
    CLOSE_SINK_WRITER_FAILED("Kafka-03", "Close kafka sink writer failed"),
    ADD_SPLIT_CHECKPOINT_FAILED("Kafka-04", "Add the split checkpoint state to reader failed"),
    ADD_SPLIT_BACK_TO_ENUMERATOR_FAILED("Kafka-05", "Add a split back to the split enumerator failed,it will only happen when a SourceReader failed"),
    CONSUME_THREAD_RUN_ERROR("Kafka-06", "Error occurred when the kafka consumer thread was running"),
    CONSUME_DATA_FAILED("Kafka-07", "Kafka failed to consume data");

    private final String code;
    private final String description;

    KafkaConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String getErrorMessage() {
        return SeaTunnelErrorCode.super.getErrorMessage();
    }
}
