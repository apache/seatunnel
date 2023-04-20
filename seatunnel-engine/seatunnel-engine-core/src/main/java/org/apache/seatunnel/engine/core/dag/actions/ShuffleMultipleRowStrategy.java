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

import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.experimental.Tolerate;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@SuperBuilder(toBuilder = true)
@Getter
@Setter
@ToString
public class ShuffleMultipleRowStrategy extends ShuffleStrategy {
    private MultipleRowType inputRowType;
    private String targetTableId;

    @Tolerate
    public ShuffleMultipleRowStrategy() {}

    @Override
    public Map<String, IQueue<Record<?>>> createShuffles(
            HazelcastInstance hazelcast, int pipelineId, int inputIndex) {
        Map<String, IQueue<Record<?>>> shuffleMap = new HashMap<>();
        for (Map.Entry<String, SeaTunnelRowType> entry : inputRowType) {
            String tableId = entry.getKey();
            String queueName = generateQueueName(pipelineId, inputIndex, tableId);
            IQueue<Record<?>> queue = getIQueue(hazelcast, queueName);
            // clear old data when job restore
            queue.clear();
            shuffleMap.put(queueName, queue);
        }

        log.info(
                "pipeline[{}] / reader[{}] assigned shuffle queue list: {}",
                pipelineId,
                inputIndex,
                shuffleMap.keySet());

        return shuffleMap;
    }

    @Override
    public String createShuffleKey(Record<?> record, int pipelineId, int inputIndex) {
        String tableId;
        if (record.getData() instanceof SeaTunnelRow) {
            tableId = ((SeaTunnelRow) record.getData()).getTableId();
        } else if (record.getData() instanceof SchemaChangeEvent) {
            tableId = ((SchemaChangeEvent) record.getData()).tablePath().toString();
        } else {
            throw new UnsupportedOperationException("Unsupported record: " + record);
        }
        return generateQueueName(pipelineId, inputIndex, tableId);
    }

    @Override
    public IQueue<Record<?>>[] getShuffles(
            HazelcastInstance hazelcast, int pipelineId, int targetIndex) {
        IQueue<Record<?>>[] queues = new IQueue[getInputPartitions()];
        for (int inputIndex = 0; inputIndex < getInputPartitions(); inputIndex++) {
            Objects.requireNonNull(targetTableId);
            String queueName = generateQueueName(pipelineId, inputIndex, targetTableId);
            queues[inputIndex] = getIQueue(hazelcast, queueName);
        }

        log.info(
                "pipeline[{}] / writer[{}] assigned shuffle queue list: {}",
                pipelineId,
                targetIndex,
                Stream.of(queues).map(e -> e.getName()).collect(Collectors.toList()));

        return queues;
    }

    private String generateQueueName(int pipelineId, int inputIndex, String tableId) {
        return "ShuffleMultipleRow-Queue_"
                + getJobId()
                + "_"
                + pipelineId
                + "_"
                + inputIndex
                + "_"
                + tableId;
    }
}
