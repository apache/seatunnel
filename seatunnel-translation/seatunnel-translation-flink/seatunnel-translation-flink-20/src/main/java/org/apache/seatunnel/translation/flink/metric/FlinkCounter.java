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

/** Flink implementation of SeaTunnel Counter metric. */
public class FlinkCounter implements Counter {
    private final String name;
    private final org.apache.flink.metrics.Counter flinkCounter;

    public FlinkCounter(String name, org.apache.flink.metrics.Counter flinkCounter) {
        this.name = name;
        this.flinkCounter = flinkCounter;
    }

    @Override
    public void inc() {
        flinkCounter.inc();
    }

    @Override
    public void inc(long n) {
        flinkCounter.inc(n);
    }

    @Override
    public void dec() {
        // Not supported by Flink counter
    }

    @Override
    public void dec(long n) {
        // Not supported by Flink counter
    }

    @Override
    public void set(long n) {
        // Not supported by Flink counter
    }

    @Override
    public long getCount() {
        return flinkCounter.getCount();
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
