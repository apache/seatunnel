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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.TablePath;

import org.apache.kafka.common.TopicPartition;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

public class KafkaSourceSplit implements SourceSplit {

    private TablePath tablePath;
    private TopicPartition topicPartition;
    private long startOffset = -1L;
    private long endOffset = -1L;
    @Setter @Getter private transient volatile boolean finish = false;

    public KafkaSourceSplit(TablePath tablePath, TopicPartition topicPartition) {
        this.tablePath = tablePath;
        this.topicPartition = topicPartition;
    }

    public KafkaSourceSplit(
            TablePath tablePath, TopicPartition topicPartition, long startOffset, long endOffset) {
        this.tablePath = tablePath;
        this.topicPartition = topicPartition;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
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

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public void setTopicPartition(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public void setTablePath(TablePath tablePath) {
        this.tablePath = tablePath;
    }

    @Override
    public String splitId() {
        return topicPartition.topic() + "-" + topicPartition.partition();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaSourceSplit that = (KafkaSourceSplit) o;
        return Objects.equals(topicPartition, that.topicPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition);
    }

    public KafkaSourceSplit copy() {
        return new KafkaSourceSplit(
                this.tablePath, this.topicPartition, this.getStartOffset(), this.getEndOffset());
    }
}
