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
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import java.util.List;

public class TransformFlowLifeCycle<T, R> implements OneInputFlowLifeCycle<R> {

    private final List<SeaTunnelTransform<T>> transform;

    private final Collector<R> collector;

    public TransformFlowLifeCycle(List<SeaTunnelTransform<T>> transform, Collector<R> collector) {
        this.transform = transform;
        this.collector = collector;
    }

    @Override
    public void received(R row) {
        T r = (T) row;
        for (SeaTunnelTransform<T> t : transform) {
            r = t.map(r);
        }
        collector.collect((R) r);
    }
}
