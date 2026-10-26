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

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.reflect.Modifier;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BenchmarkBaseTest {

    @Test
    void shouldInheritDefaultJmhSettings() {
        Class<TestBenchmark> benchmarkClass = TestBenchmark.class;

        assertTrue(Modifier.isAbstract(BenchmarkBase.class.getModifiers()));
        assertEquals(Scope.Thread, benchmarkClass.getAnnotation(State.class).value());
        assertEquals(
                TimeUnit.MILLISECONDS, benchmarkClass.getAnnotation(OutputTimeUnit.class).value());
        assertArrayEquals(
                new Mode[] {Mode.Throughput},
                benchmarkClass.getAnnotation(BenchmarkMode.class).value());
        assertEquals(3, benchmarkClass.getAnnotation(Fork.class).value());
        assertEquals(3, benchmarkClass.getAnnotation(Warmup.class).iterations());
        assertEquals(5, benchmarkClass.getAnnotation(Measurement.class).iterations());

        assertArrayEquals(
                new String[] {
                    "-Xms4g",
                    "-Xmx4g",
                    "-XX:+UseG1GC",
                    "-XX:+AlwaysPreTouch",
                    "-XX:+DisableExplicitGC",
                    "-XX:ActiveProcessorCount=4",
                    "-Djava.net.preferIPv4Stack=true"
                },
                SeaTunnelPipelineBenchmark.class.getAnnotation(Fork.class).jvmArgsAppend());
    }

    private static final class TestBenchmark extends BenchmarkBase {}
}
