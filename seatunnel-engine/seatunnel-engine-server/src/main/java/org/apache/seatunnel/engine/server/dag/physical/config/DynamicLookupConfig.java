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

package org.apache.seatunnel.engine.server.dag.physical.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Runtime queue binding for a dynamic lookup task.
 *
 * <p>M1 starts with forward-only, same-task-group input queues. Cross-worker exchange uses the
 * channel descriptors separately and must not infer queue IDs from logical edge IDs.
 */
public final class DynamicLookupConfig implements FlowConfig {

    private static final long serialVersionUID = 1L;

    /** Input queue configs keyed by target input port. */
    private final Map<Integer, IntermediateQueueConfig> inputQueues;

    private final IntermediateQueueConfig factGateCommandQueue;

    public DynamicLookupConfig(
            Map<Integer, IntermediateQueueConfig> inputQueues,
            IntermediateQueueConfig factGateCommandQueue) {
        if (inputQueues == null || inputQueues.isEmpty()) {
            throw new IllegalArgumentException("inputQueues must not be empty");
        }
        if (factGateCommandQueue == null) {
            throw new IllegalArgumentException("factGateCommandQueue must not be null");
        }
        this.inputQueues = Collections.unmodifiableMap(new HashMap<>(inputQueues));
        this.factGateCommandQueue = factGateCommandQueue;
    }

    public Map<Integer, IntermediateQueueConfig> getInputQueues() {
        return inputQueues;
    }

    public IntermediateQueueConfig getInputQueue(int inputPort) {
        IntermediateQueueConfig queueConfig = inputQueues.get(inputPort);
        if (queueConfig == null) {
            throw new IllegalArgumentException("Missing dynamic lookup input queue: " + inputPort);
        }
        return queueConfig;
    }

    public IntermediateQueueConfig getFactGateCommandQueue() {
        return factGateCommandQueue;
    }
}
