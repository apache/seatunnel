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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.transform.Collector;

import java.util.concurrent.BlockingQueue;

public class IntermediateQueueFlowLifeCycle<T> implements OneInputFlowLifeCycle<T>, OneOutputFlowLifeCycle<T> {

    private final BlockingQueue<T> queue;

    public IntermediateQueueFlowLifeCycle(BlockingQueue<T> queue) {
        this.queue = queue;
    }

    @Override
    public void received(T row) {
        try {
            queue.put(row);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void collect(Collector<T> collector) throws Exception {
        while (true) {
            T record = queue.poll();
            if (record != null) {
                collector.collect(record);
            } else {
                break;
            }
        }
    }
}
