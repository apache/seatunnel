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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.sink;

import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqAdminUtil;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqBaseConfiguration;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import lombok.extern.slf4j.Slf4j;

import static org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorErrorCode.PRODUCER_SEND_MESSAGE_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorErrorCode.PRODUCER_START_ERROR;

@Slf4j
public class RocketMqNoTransactionSender implements RocketMqProducerSender {

    private final DefaultMQProducer rocketMqProducer;
    private final boolean isSync;

    public RocketMqNoTransactionSender(RocketMqBaseConfiguration configuration, boolean isSync) {
        this.isSync = isSync;
        this.rocketMqProducer = RocketMqAdminUtil.initDefaultMqProducer(configuration);
        try {
            this.rocketMqProducer.start();
        } catch (MQClientException e) {
            throw new RocketMqConnectorException(PRODUCER_START_ERROR, e);
        }
    }

    @Override
    public void send(Message message) {
        if (message == null) {
            return;
        }
        try {
            if (isSync) {
                if (StringUtils.isEmpty(message.getKeys())) {
                    this.rocketMqProducer.send(message);
                } else {
                    this.rocketMqProducer.send(
                            message, new SelectMessageQueueByHash(), message.getKeys());
                }
            } else {
                SendCallback callback =
                        new SendCallback() {
                            @Override
                            public void onSuccess(SendResult sendResult) {
                                // No-op
                            }

                            @Override
                            public void onException(Throwable e) {
                                log.error("Failed to send data to rocketmq", e);
                            }
                        };
                if (StringUtils.isEmpty(message.getKeys())) {
                    this.rocketMqProducer.send(message, callback);
                } else {
                    this.rocketMqProducer.send(
                            message, new SelectMessageQueueByHash(), message.getKeys(), callback);
                }
            }
        } catch (MQClientException
                | RemotingException
                | InterruptedException
                | MQBrokerException e) {
            throw new RocketMqConnectorException(PRODUCER_SEND_MESSAGE_ERROR, e);
        }
    }

    @Override
    public void close() throws Exception {
        if (rocketMqProducer != null) {
            this.rocketMqProducer.shutdown();
        }
    }
}
