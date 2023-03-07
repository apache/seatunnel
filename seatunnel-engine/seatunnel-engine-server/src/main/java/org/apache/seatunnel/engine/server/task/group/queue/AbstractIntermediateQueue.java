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
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.flow.IntermediateQueueFlowLifeCycle;

import lombok.Getter;
import lombok.Setter;

import java.io.IOException;

public abstract class AbstractIntermediateQueue<T> {

    @Getter @Setter private SeaTunnelTask runningTask;

    @Getter @Setter private IntermediateQueueFlowLifeCycle<?> intermediateQueueFlowLifeCycle;

    private final T queue;

    public AbstractIntermediateQueue(T queue) {
        this.queue = queue;
    }

    public T getIntermediateQueue() {
        return queue;
    }

    public abstract void received(Record<?> record);

    public abstract void collect(Collector<Record<?>> collector) throws Exception;

    public abstract void close() throws IOException;
}
