/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.Objects;

/** A {@link SourceSplit} for a RocketMQ partition. */
public class RocketMQPartitionSplit implements SourceSplit {

    private static final long serialVersionUID = -928041877587828579L;
    private MessageQueue messageQueue;
    private long startOffset = -1;
    private long endOffset = -1;

    public RocketMQPartitionSplit(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public RocketMQPartitionSplit(MessageQueue messageQueue, long startOffset, long endOffset) {
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

    public long getEndOffset() {
        return endOffset;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    @Override
    public String splitId() {
        return messageQueue.getTopic()
                + "-"
                + messageQueue.getBrokerName()
                + "-"
                + messageQueue.getQueueId();
    }

    @Override
    public String toString() {
        return String.format(
                "[Topic: %s, Broker: %s, Partition: %s, StartOffset: %d, EndOffset: %d]",
                messageQueue.getTopic(),
                messageQueue.getBrokerName(),
                messageQueue.getQueueId(),
                startOffset,
                endOffset);
    }

    public static String toSplitId(MessageQueue messageQueue) {
        return messageQueue.getTopic()
                + "-"
                + messageQueue.getBrokerName()
                + "-"
                + messageQueue.getQueueId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RocketMQPartitionSplit that = (RocketMQPartitionSplit) o;
        return Objects.equals(messageQueue, that.messageQueue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageQueue);
    }

    public RocketMQPartitionSplit copy() {
        return new RocketMQPartitionSplit(this.messageQueue);
    }
}
