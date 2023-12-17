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
import com.hazelcast.core.HazelcastInstance;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.experimental.Tolerate;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Slf4j
@SuperBuilder
@Getter
@Setter
@ToString
public class ShufflePartitionStrategy extends ShuffleStrategy {
    private final Map<Integer, String[]> inputQueueMapping = new HashMap<>();
    private int targetPartitions;

    @Tolerate
    public ShufflePartitionStrategy() {}

    @Override
    public Map<String, IQueue<Record<?>>> createShuffles(
            HazelcastInstance hazelcast, int pipelineId, int inputIndex) {
        checkArgument(inputIndex >= 0 && inputIndex < getInputPartitions());
        Map<String, IQueue<Record<?>>> shuffleMap = new LinkedHashMap<>();
        for (int targetIndex = 0; targetIndex < targetPartitions; targetIndex++) {
            String queueName = generateQueueName(pipelineId, inputIndex, targetIndex);
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
        String[] inputQueueNames =
                inputQueueMapping.computeIfAbsent(
                        inputIndex,
                        key -> {
                            String[] queueNames = new String[targetPartitions];
                            for (int targetIndex = 0;
                                    targetIndex < targetPartitions;
                                    targetIndex++) {
                                queueNames[targetIndex] =
                                        generateQueueName(pipelineId, key, targetIndex);
                            }
                            return queueNames;
                        });
        return inputQueueNames[ThreadLocalRandom.current().nextInt(targetPartitions)];
    }

    @Override
    public IQueue<Record<?>>[] getShuffles(
            HazelcastInstance hazelcast, int pipelineId, int targetIndex) {
        checkArgument(targetIndex >= 0 && targetIndex < targetPartitions);
        IQueue<Record<?>>[] shuffles = new IQueue[getInputPartitions()];
        for (int inputIndex = 0; inputIndex < getInputPartitions(); inputIndex++) {
            String queueName = generateQueueName(pipelineId, inputIndex, targetIndex);
            shuffles[inputIndex] = getIQueue(hazelcast, queueName);
        }

        log.info(
                "pipeline[{}] / writer[{}] assigned shuffle queue list: {}",
                pipelineId,
                targetIndex,
                Stream.of(shuffles).map(e -> e.getName()).collect(Collectors.toList()));

        return shuffles;
    }

    private String generateQueueName(int pipelineId, int inputIndex, int targetIndex) {
        return String.format(
                "ShufflePartition-Queue_%s_%s_%s_%s",
                getJobId(), pipelineId, inputIndex, targetIndex);
    }
}
