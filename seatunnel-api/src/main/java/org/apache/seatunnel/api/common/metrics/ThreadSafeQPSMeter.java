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

package org.apache.seatunnel.api.common.metrics;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class ThreadSafeQPSMeter implements Meter, Serializable {

    private static final long serialVersionUID = 1L;

    private static final AtomicLongFieldUpdater<ThreadSafeQPSMeter> VOLATILE_VALUE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ThreadSafeQPSMeter.class, "value");

    private final String name;

    private volatile long value;

    private final long timestamp;

    public ThreadSafeQPSMeter(String name) {
        this.name = name;
        timestamp = System.currentTimeMillis();
    }

    @Override
    public void markEvent() {
        VOLATILE_VALUE_UPDATER.incrementAndGet(this);
    }

    @Override
    public void markEvent(long n) {
        VOLATILE_VALUE_UPDATER.addAndGet(this, n);
    }

    @Override
    public double getRate() {
        long cost = System.currentTimeMillis() - timestamp;
        return (double) value * 1000 / cost;
    }

    @Override
    public long getCount() {
        return VOLATILE_VALUE_UPDATER.get(this);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Unit unit() {
        return Unit.COUNT;
    }

    @Override
    public String toString() {
        return "ThreadSafeQPSMeter{"
                + "name='"
                + name
                + '\''
                + ", value="
                + value
                + ", timestamp="
                + timestamp
                + '}';
    }
}
