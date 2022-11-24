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

import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.common.utils.function.ConsumerWithException;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class IntermediateQueueFlowLifeCycle extends AbstractFlowLifeCycle implements OneInputFlowLifeCycle<Record<?>>,
        OneOutputFlowLifeCycle<Record<?>> {

    private final BlockingQueue<Record<?>> queue;

    public IntermediateQueueFlowLifeCycle(SeaTunnelTask runningTask,
                                          CompletableFuture<Void> completableFuture,
                                          BlockingQueue<Record<?>> queue) {
        super(runningTask, completableFuture);
        this.queue = queue;
    }

    @Override
    public void received(Record<?> record) {
        try {
            handleRecord(record, queue::put);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void collect(Collector<Record<?>> collector) throws Exception {
        while (true) {
            Record<?> record = queue.poll(100, TimeUnit.MILLISECONDS);
            if (record != null) {
                handleRecord(record, collector::collect);
            } else {
                break;
            }
        }
    }

    private void handleRecord(Record<?> record, ConsumerWithException<Record<?>> consumer) throws Exception {
        if (record.getData() instanceof Barrier) {
            CheckpointBarrier barrier = (CheckpointBarrier) record.getData();
            runningTask.ack(barrier);
            if (barrier.prepareClose()) {
                prepareClose = true;
            }
            consumer.accept(record);
        } else {
            if (prepareClose) {
                return;
            }
            consumer.accept(record);
        }
    }
}
