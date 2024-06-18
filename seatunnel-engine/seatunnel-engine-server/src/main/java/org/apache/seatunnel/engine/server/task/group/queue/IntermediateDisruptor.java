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

package org.apache.seatunnel.engine.server.task.group.queue;

import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.engine.server.task.group.queue.disruptor.RecordEvent;
import org.apache.seatunnel.engine.server.task.group.queue.disruptor.RecordEventHandler;
import org.apache.seatunnel.engine.server.task.group.queue.disruptor.RecordEventProducer;

import com.lmax.disruptor.dsl.Disruptor;

import java.io.IOException;

public class IntermediateDisruptor extends AbstractIntermediateQueue<Disruptor<RecordEvent>> {

    public IntermediateDisruptor(Disruptor<RecordEvent> queue) {
        super(queue);
    }

    private volatile boolean isExecuted;

    @Override
    public void received(Record<?> record) {
        getIntermediateQueue().getRingBuffer();
        RecordEventProducer.onData(
                record,
                getIntermediateQueue().getRingBuffer(),
                getIntermediateQueueFlowLifeCycle());
    }

    @Override
    public void collect(Collector<Record<?>> collector) throws Exception {
        if (!isExecuted) {
            getIntermediateQueue()
                    .handleEventsWith(
                            new RecordEventHandler(
                                    getRunningTask(),
                                    collector,
                                    getIntermediateQueueFlowLifeCycle()));
            getIntermediateQueue().start();
            isExecuted = true;
        } else {
            Thread.sleep(100);
        }
    }

    @Override
    public void close() throws IOException {
        getIntermediateQueue().shutdown();
    }
}
