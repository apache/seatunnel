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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Objects;

/** Configuration for connecting RocketMq */
@Setter
@Getter
public class RocketMqBaseConfiguration implements Serializable {
    private String namesrvAddr;
    private String groupId;
    /** set acl config */
    private boolean aclEnable;

    private String accessKey;
    private String secretKey;

    // consumer
    private Integer batchSize;
    private Long pollTimeoutMillis;

    // producer
    private Integer maxMessageSize;
    private Integer sendMsgTimeout;

    private RocketMqBaseConfiguration(
            String groupId,
            String namesrvAddr,
            boolean aclEnable,
            String accessKey,
            String secretKey) {
        this.groupId = groupId;
        this.namesrvAddr = namesrvAddr;
        this.aclEnable = aclEnable;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    private RocketMqBaseConfiguration(
            String groupId,
            String namesrvAddr,
            boolean aclEnable,
            String accessKey,
            String secretKey,
            int pullBatchSize,
            Long consumerPullTimeoutMillis) {
        this(groupId, namesrvAddr, aclEnable, accessKey, secretKey);
        this.batchSize = pullBatchSize;
        this.pollTimeoutMillis = consumerPullTimeoutMillis;
    }

    private RocketMqBaseConfiguration(
            String groupId,
            String namesrvAddr,
            boolean aclEnable,
            String accessKey,
            String secretKey,
            int maxMessageSize,
            int sendMsgTimeout) {

        this(groupId, namesrvAddr, aclEnable, accessKey, secretKey);
        this.maxMessageSize = maxMessageSize;
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RocketMqBaseConfiguration that = (RocketMqBaseConfiguration) o;
        return aclEnable == that.aclEnable
                && batchSize == that.batchSize
                && pollTimeoutMillis == that.pollTimeoutMillis
                && maxMessageSize == that.maxMessageSize
                && sendMsgTimeout == that.sendMsgTimeout
                && Objects.equals(namesrvAddr, that.namesrvAddr)
                && Objects.equals(groupId, that.groupId)
                && Objects.equals(accessKey, that.accessKey)
                && Objects.equals(secretKey, that.secretKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                namesrvAddr,
                groupId,
                aclEnable,
                accessKey,
                secretKey,
                batchSize,
                pollTimeoutMillis,
                maxMessageSize,
                sendMsgTimeout);
    }

    @Override
    public String toString() {
        return "RocketMqBaseConfiguration{"
                + "namesrvAddr='"
                + namesrvAddr
                + '\''
                + ", groupId='"
                + groupId
                + '\''
                + ", aclEnable="
                + aclEnable
                + ", accessKey='"
                + accessKey
                + '\''
                + ", secretKey='"
                + secretKey
                + '\''
                + ", pullBatchSize="
                + batchSize
                + ", pollTimeoutMillis="
                + pollTimeoutMillis
                + ", maxMessageSize="
                + maxMessageSize
                + ", sendMsgTimeout="
                + sendMsgTimeout
                + '}';
    }

    enum ConfigType {
        NONE,
        CONSUMER,
        PRODUCER
    }

    public static class Builder {
        private String namesrvAddr;
        private String groupId;
        private boolean aclEnable;
        private String accessKey;
        private String secretKey;
        // consumer
        private Integer batchSize;
        private Long pollTimeoutMillis;

        // producer
        private Integer maxMessageSize;
        private Integer sendMsgTimeout;

        private ConfigType configType = ConfigType.NONE;

        public Builder consumer() {
            this.configType = ConfigType.CONSUMER;
            return this;
        }

        public Builder producer() {
            this.configType = ConfigType.PRODUCER;
            return this;
        }

        public Builder namesrvAddr(String namesrvAddr) {
            this.namesrvAddr = namesrvAddr;
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder aclEnable(boolean aclEnable) {
            this.aclEnable = aclEnable;
            return this;
        }

        public Builder accessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder secretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder pollTimeoutMillis(long consumerPullTimeoutMillis) {
            this.pollTimeoutMillis = consumerPullTimeoutMillis;
            return this;
        }

        public Builder maxMessageSize(int maxMessageSize) {
            this.maxMessageSize = maxMessageSize;
            return this;
        }

        public Builder sendMsgTimeout(int sendMsgTimeout) {
            this.sendMsgTimeout = sendMsgTimeout;
            return this;
        }

        public RocketMqBaseConfiguration build() {
            switch (configType) {
                case CONSUMER:
                    return new RocketMqBaseConfiguration(
                            groupId,
                            namesrvAddr,
                            aclEnable,
                            accessKey,
                            secretKey,
                            batchSize,
                            pollTimeoutMillis);
                case PRODUCER:
                    return new RocketMqBaseConfiguration(
                            groupId,
                            namesrvAddr,
                            aclEnable,
                            accessKey,
                            secretKey,
                            maxMessageSize,
                            sendMsgTimeout);
                default:
                    return new RocketMqBaseConfiguration(
                            groupId, namesrvAddr, aclEnable, accessKey, secretKey);
            }
        }
    }
}
