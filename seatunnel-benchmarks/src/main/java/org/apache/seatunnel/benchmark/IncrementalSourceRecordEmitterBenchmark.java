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

package org.apache.seatunnel.benchmark;

import org.apache.seatunnel.benchmark.cdc.IncrementalSourceRecordEmitterFixture;

import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** Measures the incremental CDC stream filter and record emitter with prepared events. */
@Threads(1)
@Fork(
        value = 3,
        jvmArgsAppend = {
            "-Xms4g",
            "-Xmx4g",
            "-XX:+UseG1GC",
            "-XX:+AlwaysPreTouch",
            "-XX:+DisableExplicitGC",
            "-XX:ActiveProcessorCount=4",
            "-Djava.net.preferIPv4Stack=true"
        })
public class IncrementalSourceRecordEmitterBenchmark extends BenchmarkBase {

    @Param({"5", "10", "15", "20"})
    private int columns;

    @Param({"INSERT", "UPDATE"})
    private IncrementalSourceRecordEmitterFixture.Operation operation;

    @Param({"false", "true"})
    private boolean exactlyOnce;

    private IncrementalSourceRecordEmitterFixture fixture;

    public static void main(String[] args) throws IOException {
        String[] options = new String[args.length + 1];
        options[0] = IncrementalSourceRecordEmitterBenchmark.class.getCanonicalName();
        System.arraycopy(args, 0, options, 1, args.length);
        Main.main(options);
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        fixture = new IncrementalSourceRecordEmitterFixture(columns, operation, exactlyOnce);
        fixture.setUp();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.SECONDS)
    @OperationsPerInvocation(IncrementalSourceRecordEmitterFixture.BATCH_EVENTS)
    public int filterAndEmit() throws Exception {
        return fixture.filterAndEmit();
    }

    @TearDown(Level.Iteration)
    public void verifyIteration() {
        fixture.verifyBatch();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (fixture != null) {
            fixture.close();
        }
    }
}
