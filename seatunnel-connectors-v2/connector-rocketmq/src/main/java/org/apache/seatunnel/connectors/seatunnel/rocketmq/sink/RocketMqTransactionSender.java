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
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import static org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorErrorCode.PRODUCER_SEND_MESSAGE_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorErrorCode.PRODUCER_START_ERROR;

public class RocketMqTransactionSender implements RocketMqProducerSender {

    private static final String TXN_PARAM = "SeaTunnel-RocketMq";
    private final TransactionMQProducer transactionMQProducer;

    public RocketMqTransactionSender(RocketMqBaseConfiguration configuration) {
        this.transactionMQProducer =
                RocketMqAdminUtil.initTransactionMqProducer(
                        configuration,
                        new TransactionListener() {
                            @Override
                            public LocalTransactionState executeLocalTransaction(
                                    Message msg, Object arg) {
                                return LocalTransactionState.COMMIT_MESSAGE;
                            }

                            @Override
                            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                                return LocalTransactionState.COMMIT_MESSAGE;
                            }
                        });
        try {
            this.transactionMQProducer.start();
        } catch (MQClientException e) {
            throw new RocketMqConnectorException(PRODUCER_START_ERROR, e);
        }
    }

    @Override
    public void send(Message message) {
        try {
            transactionMQProducer.sendMessageInTransaction(
                    message,
                    StringUtils.isEmpty(message.getKeys()) ? TXN_PARAM : message.getKeys());
        } catch (MQClientException e) {
            throw new RocketMqConnectorException(PRODUCER_SEND_MESSAGE_ERROR, e);
        }
    }

    @Override
    public void close() throws Exception {
        if (transactionMQProducer != null) {
            this.transactionMQProducer.shutdown();
        }
    }
}
