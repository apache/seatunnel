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

package org.apache.seatunnel.engine.e2e.failurecapacity;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sink writer whose failure is controlled by the test.
 *
 * <p>{@link #WRITER_INSTANCES} counts how many sink writers the engine has created for the job,
 * i.e. how many times the pipeline was (re)deployed, which is the observable retry budget. While
 * {@link #ARMED} is {@code false} the writer accepts rows so the pipeline can reach a steady
 * RUNNING state and complete real checkpoints; once the test arms it, every row fails the task.
 */
public class ArmableFailingSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    public static final AtomicBoolean ARMED = new AtomicBoolean(false);
    public static final AtomicInteger WRITER_INSTANCES = new AtomicInteger();
    public static final AtomicInteger WRITTEN_ROWS = new AtomicInteger();

    private final int writerInstance = WRITER_INSTANCES.incrementAndGet();

    @Override
    public void write(SeaTunnelRow element) {
        if (ARMED.get()) {
            throw new IllegalStateException(
                    "armable-failing-sink: injected failure in writer instance " + writerInstance);
        }
        WRITTEN_ROWS.incrementAndGet();
    }

    @Override
    public void close() {}

    public static void reset() {
        ARMED.set(false);
        WRITER_INSTANCES.set(0);
        WRITTEN_ROWS.set(0);
    }
}
