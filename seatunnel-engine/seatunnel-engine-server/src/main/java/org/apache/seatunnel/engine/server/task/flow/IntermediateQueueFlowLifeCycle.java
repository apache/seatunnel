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
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.group.disruptor.RecordEvent;
import org.apache.seatunnel.engine.server.task.group.disruptor.RecordEventHandler;
import org.apache.seatunnel.engine.server.task.group.disruptor.RecordEventProducer;

import com.lmax.disruptor.dsl.Disruptor;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class IntermediateQueueFlowLifeCycle extends AbstractFlowLifeCycle implements OneInputFlowLifeCycle<Record<?>>,
    OneOutputFlowLifeCycle<Record<?>> {

    private final Disruptor<RecordEvent> disruptor;

    private volatile boolean isExecuted;

    public IntermediateQueueFlowLifeCycle(SeaTunnelTask runningTask,
                                          CompletableFuture<Void> completableFuture,
                                          Disruptor<RecordEvent> disruptor) {
        super(runningTask, completableFuture);
        this.disruptor = disruptor;
    }

    @Override
    public void received(Record<?> record) {
        RecordEventProducer.onData(record, disruptor.getRingBuffer(), this);
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void collect(Collector<Record<?>> collector) throws Exception {
        if (!isExecuted) {
            disruptor.handleEventsWith(new RecordEventHandler(runningTask, collector, this));
            disruptor.start();
            isExecuted = true;
        } else {
            Thread.sleep(100);
        }
    }

    @Override
    public void close() throws IOException {
        disruptor.shutdown();
        super.close();
    }
}
