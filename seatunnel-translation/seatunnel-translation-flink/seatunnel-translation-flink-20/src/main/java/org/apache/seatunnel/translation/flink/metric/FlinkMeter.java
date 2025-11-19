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

import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.Unit;

/** Flink implementation of SeaTunnel Meter metric. */
public class FlinkMeter implements Meter {
    private final String name;
    private final org.apache.flink.metrics.Meter flinkMeter;

    public FlinkMeter(String name, org.apache.flink.metrics.Meter flinkMeter) {
        this.name = name;
        this.flinkMeter = flinkMeter;
    }

    @Override
    public void markEvent() {
        flinkMeter.markEvent();
    }

    @Override
    public void markEvent(long n) {
        flinkMeter.markEvent(n);
    }

    @Override
    public double getRate() {
        return flinkMeter.getRate();
    }

    @Override
    public long getCount() {
        return flinkMeter.getCount();
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
