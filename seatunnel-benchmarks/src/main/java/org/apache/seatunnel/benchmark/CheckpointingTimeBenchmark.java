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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static java.util.concurrent.TimeUnit.SECONDS;

/** Measures regular checkpoint completion time for one input pipeline. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(SECONDS)
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
public class CheckpointingTimeBenchmark extends BenchmarkBase {

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(CheckpointingTimeBenchmark.class.getCanonicalName())
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    public void checkpointSingleInput(CheckpointingTimeBenchmarkPipeline pipeline)
            throws Exception {
        pipeline.triggerCheckpoint();
    }
}
