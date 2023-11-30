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

import org.apache.seatunnel.api.source.SourceSplit;

import org.apache.rocketmq.common.message.MessageQueue;

/** define rocketmq source split */
public class RocketMqSourceSplit implements SourceSplit {
    private MessageQueue messageQueue;
    private long startOffset = -1L;
    private long endOffset = -1L;

    public RocketMqSourceSplit() {}

    public RocketMqSourceSplit(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public RocketMqSourceSplit(MessageQueue messageQueue, long startOffset, long endOffset) {
        this.messageQueue = messageQueue;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    @Override
    public String splitId() {
        return this.messageQueue.getTopic()
                + "-"
                + this.messageQueue.getBrokerName()
                + "-"
                + this.messageQueue.getQueueId();
    }

    public RocketMqSourceSplit copy() {
        return new RocketMqSourceSplit(
                this.messageQueue, this.getStartOffset(), this.getEndOffset());
    }
}
