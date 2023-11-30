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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.source;

import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqAdminUtil;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class RocketMqConsumerThread implements Runnable {
    private final DefaultLitePullConsumer consumer;
    private final ConsumerMetadata metadata;
    private final LinkedBlockingQueue<Consumer<DefaultLitePullConsumer>> tasks;

    public RocketMqConsumerThread(ConsumerMetadata metadata) {
        this.metadata = metadata;
        this.tasks = new LinkedBlockingQueue<>();
        this.consumer =
                RocketMqAdminUtil.initDefaultLitePullConsumer(
                        this.metadata.getBaseConfig(), !metadata.isEnabledCommitCheckpoint());
        try {
            this.consumer.start();
        } catch (MQClientException e) {
            // Start rocketmq failed
            throw new RocketMqConnectorException(
                    RocketMqConnectorErrorCode.CONSUMER_START_ERROR, e);
        }
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Consumer<DefaultLitePullConsumer> task = tasks.poll(1, TimeUnit.SECONDS);
                    if (task != null) {
                        task.accept(consumer);
                    }
                } catch (InterruptedException e) {
                    throw new RocketMqConnectorException(
                            RocketMqConnectorErrorCode.CONSUME_THREAD_RUN_ERROR, e);
                }
            }
        } finally {
            this.consumer.shutdown();
        }
    }

    public LinkedBlockingQueue<Consumer<DefaultLitePullConsumer>> getTasks() {
        return tasks;
    }
}
