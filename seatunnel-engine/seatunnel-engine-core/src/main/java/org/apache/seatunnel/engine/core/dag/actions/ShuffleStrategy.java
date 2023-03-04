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

package org.apache.seatunnel.engine.core.dag.actions;

import org.apache.seatunnel.api.table.type.Record;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.HazelcastInstance;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.experimental.Tolerate;

import java.io.Serializable;
import java.util.Map;

@SuperBuilder(toBuilder = true)
@Getter
@Setter
@ToString
public abstract class ShuffleStrategy implements Serializable {
    private static final int DEFAULT_QUEUE_SIZE = 2048;
    private static final int DEFAULT_QUEUE_BACKUP_COUNT = 0;
    private static final int DEFAULT_QUEUE_ASYNC_BACKUP_COUNT = 0;

    protected long jobId;
    protected int inputPartitions;
    @Builder.Default protected int queueMaxSize = DEFAULT_QUEUE_SIZE;
    @Builder.Default protected int queueBackupCount = DEFAULT_QUEUE_BACKUP_COUNT;
    @Builder.Default protected int queueAsyncBackupCount = DEFAULT_QUEUE_ASYNC_BACKUP_COUNT;
    protected int queueEmptyQueueTtl;

    @Tolerate
    public ShuffleStrategy() {}

    public abstract Map<String, IQueue<Record<?>>> createShuffles(
            HazelcastInstance hazelcast, int pipelineId, int inputIndex);

    public abstract String createShuffleKey(Record<?> record, int pipelineId, int inputIndex);

    public abstract IQueue<Record<?>>[] getShuffles(
            HazelcastInstance hazelcast, int pipelineId, int targetIndex);

    protected IQueue<Record<?>> getIQueue(HazelcastInstance hazelcast, String queueName) {
        QueueConfig targetQueueConfig = hazelcast.getConfig().getQueueConfig(queueName);
        targetQueueConfig.setMaxSize(queueMaxSize);
        targetQueueConfig.setBackupCount(queueBackupCount);
        targetQueueConfig.setAsyncBackupCount(queueAsyncBackupCount);
        targetQueueConfig.setEmptyQueueTtl(queueEmptyQueueTtl);
        return hazelcast.getQueue(queueName);
    }
}
