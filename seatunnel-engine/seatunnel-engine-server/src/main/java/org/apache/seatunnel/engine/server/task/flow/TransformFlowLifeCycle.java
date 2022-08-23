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
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.server.task.TaskRuntimeException;
import org.apache.seatunnel.engine.server.task.record.ClosedSign;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TransformFlowLifeCycle<T> extends AbstractFlowLifeCycle implements OneInputFlowLifeCycle<Record<?>> {

    private final List<SeaTunnelTransform<T>> transform;

    private final Collector<Record<?>> collector;

    public TransformFlowLifeCycle(List<SeaTunnelTransform<T>> transform, Collector<Record<?>> collector,
                                  CompletableFuture<Void> completableFuture) {
        super(completableFuture);
        this.transform = transform;
        this.collector = collector;
    }

    @Override
    public void received(Record<?> row) {
        if (row.getData() instanceof ClosedSign) {
            collector.collect(row);
            try {
                this.close();
            } catch (Exception e) {
                throw new TaskRuntimeException(e);
            }
        } else {
            T r = (T) row.getData();
            for (SeaTunnelTransform<T> t : transform) {
                r = t.map(r);
            }
            collector.collect(new Record<>(r));
        }
    }
}
