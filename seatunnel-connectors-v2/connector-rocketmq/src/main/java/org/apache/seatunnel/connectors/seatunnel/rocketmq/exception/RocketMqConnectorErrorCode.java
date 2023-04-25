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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum RocketMqConnectorErrorCode implements SeaTunnelErrorCode {
    ADD_SPLIT_BACK_TO_ENUMERATOR_FAILED(
            "ROCKETMQ-01",
            "Add a split back to the split enumerator failed, it will only happen when a SourceReader failed"),
    ADD_SPLIT_CHECKPOINT_FAILED("ROCKETMQ-02", "Add the split checkpoint state to reader failed"),
    CONSUME_DATA_FAILED("ROCKETMQ-03", "Rocketmq failed to consume data"),
    CONSUME_THREAD_RUN_ERROR(
            "ROCKETMQ-04", "Error occurred when the rocketmq consumer thread was running"),
    PRODUCER_SEND_MESSAGE_ERROR("ROCKETMQ-05", "Rocketmq producer failed to send message"),
    PRODUCER_START_ERROR("ROCKETMQ-06", "Rocketmq producer failed to start"),
    CONSUMER_START_ERROR("ROCKETMQ-07", "Rocketmq consumer failed to start"),

    UNSUPPORTED_START_MODE_ERROR("ROCKETMQ-08", "Unsupported start mode"),

    GET_CONSUMER_GROUP_OFFSETS_ERROR(
            "ROCKETMQ-09", "Failed to get the offsets of the current consumer group"),

    GET_CONSUMER_GROUP_OFFSETS_TIMESTAMP_ERROR(
            "ROCKETMQ-10", "Failed to search offset through timestamp"),

    GET_MIN_AND_MAX_OFFSETS_ERROR("ROCKETMQ-11", "Failed to get topic min and max topic"),

    TOPIC_NOT_EXIST_ERROR("ROCKETMQ-12", "Check the topic for errors"),

    CREATE_TOPIC_ERROR("ROCKETMQ-13", "Failed to create topic"),
    ;

    private final String code;
    private final String description;

    RocketMqConnectorErrorCode(String code, String description) {
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
