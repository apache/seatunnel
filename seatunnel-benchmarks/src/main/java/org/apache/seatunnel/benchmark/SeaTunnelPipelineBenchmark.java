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
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

/** Measures complete bounded pipelines on an embedded single-node Zeta cluster. */
@OperationsPerInvocation(SeaTunnelPipelineBenchmark.RECORDS_PER_INVOCATION)
@OutputTimeUnit(TimeUnit.SECONDS)
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
public class SeaTunnelPipelineBenchmark extends BenchmarkBase {

    public static final int RECORDS_PER_INVOCATION = 1_000_000;

    @Param("600000")
    private long offeredRatePerSecond;

    @Param("4")
    private int parallelism;

    @Param("256")
    private int payloadSize;

    @Param("64")
    private int transformOperations;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + SeaTunnelPipelineBenchmark.class.getCanonicalName() + ".*")
                        .build();
        new Runner(options).run();
    }

    @Benchmark
    public BenchmarkRunResult sourceSink(SeaTunnelEnvironmentContext context) throws Exception {
        return context.execute(BenchmarkPipeline.SOURCE_SINK, benchmarkOptions());
    }

    @Benchmark
    public BenchmarkRunResult sourceTransformSink(SeaTunnelEnvironmentContext context)
            throws Exception {
        return context.execute(BenchmarkPipeline.SOURCE_TRANSFORM_SINK, benchmarkOptions());
    }

    @Benchmark
    public BenchmarkRunResult sourceTransformSinkWithObservability(
            SeaTunnelObservabilityEnvironmentContext context) throws Exception {
        return context.execute(
                BenchmarkPipeline.SOURCE_TRANSFORM_SINK_OBSERVABILITY, benchmarkOptions());
    }

    @Benchmark
    public BenchmarkRunResult sourceTransformSinkWithTrace(SeaTunnelTraceEnvironmentContext context)
            throws Exception {
        return context.execute(BenchmarkPipeline.SOURCE_TRANSFORM_SINK_TRACE, benchmarkOptions());
    }

    @Benchmark
    public BenchmarkRunResult sourceTransformSinkWithObservabilityAndTrace(
            SeaTunnelObservabilityTraceEnvironmentContext context) throws Exception {
        return context.execute(
                BenchmarkPipeline.SOURCE_TRANSFORM_SINK_OBSERVABILITY_TRACE, benchmarkOptions());
    }

    private PipelineBenchmarkOptions benchmarkOptions() {
        return new PipelineBenchmarkOptions(
                RECORDS_PER_INVOCATION,
                offeredRatePerSecond,
                parallelism,
                payloadSize,
                transformOperations);
    }
}
