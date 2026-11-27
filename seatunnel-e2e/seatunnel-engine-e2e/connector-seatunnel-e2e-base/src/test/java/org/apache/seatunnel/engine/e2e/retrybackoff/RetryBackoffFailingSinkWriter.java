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

package org.apache.seatunnel.engine.e2e.retrybackoff;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sink writer that always fails a task on its first record, so the pipeline reaches {@code FAILED}
 * and enters the engine's restore backoff.
 *
 * <p>It exists only as a deterministic failure fixture: no connector, format or transform semantics
 * are under test here. {@link #OPENED} counts how many writers the engine actually created, which
 * is how the test tells "the authorized restore never deployed anything" apart from "another
 * attempt was deployed and then torn down".
 */
public class RetryBackoffFailingSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    /** Number of writers the engine has created across all attempts of the job. */
    public static final AtomicInteger OPENED = new AtomicInteger();

    public RetryBackoffFailingSinkWriter() {
        OPENED.incrementAndGet();
    }

    @Override
    public void write(SeaTunnelRow element) {
        throw new IllegalStateException("RetryBackoffFail sink always fails on write");
    }

    @Override
    public void close() {}

    public static void reset() {
        OPENED.set(0);
    }
}
