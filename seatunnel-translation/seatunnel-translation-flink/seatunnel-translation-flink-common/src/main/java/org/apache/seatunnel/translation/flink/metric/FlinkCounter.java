/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.seatunnel.translation.flink.metric;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Unit;

import org.apache.flink.api.common.accumulators.LongCounter;

public class FlinkCounter implements Counter {

    private final String name;

    private final LongCounter longCounter;

    public FlinkCounter(String name, LongCounter longCounter) {
        this.name = name;
        this.longCounter = longCounter;
    }

    @Override
    public void inc() {
        inc(1L);
    }

    @Override
    public void inc(long n) {
        longCounter.add(n);
    }

    @Override
    public void dec() {
        throw new UnsupportedOperationException("Flink metrics does not support dec operation");
    }

    @Override
    public void dec(long n) {
        throw new UnsupportedOperationException("Flink metrics does not support dec operation");
    }

    @Override
    public void set(long n) {
        longCounter.add(n);
    }

    @Override
    public long getCount() {
        return longCounter.getLocalValue();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Flink metrics does not support clear operation");
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Unit unit() {
        return Unit.COUNT;
    }
}
