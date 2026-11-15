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

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class ProtoStuffSerializerBenchmarkTest {

    @Test
    void shouldExerciseConcurrentSchemaLookupByDefault() {
        assertEquals(4, ProtoStuffSerializerBenchmark.class.getAnnotation(Threads.class).value());
        assertArrayEquals(
                new Mode[] {Mode.Throughput},
                ProtoStuffSerializerBenchmark.class.getAnnotation(BenchmarkMode.class).value());
        assertEquals(
                TimeUnit.MILLISECONDS,
                ProtoStuffSerializerBenchmark.class.getAnnotation(OutputTimeUnit.class).value());
        Fork fork = ProtoStuffSerializerBenchmark.class.getAnnotation(Fork.class);
        assertEquals(3, fork.value());
        assertArrayEquals(
                SeaTunnelPipelineBenchmark.class.getAnnotation(Fork.class).jvmArgsAppend(),
                fork.jvmArgsAppend());
    }

    @Test
    void shouldRoundTripTheWalEnvelopeAndItsPreparedPayload() {
        ProtoStuffSerializerBenchmark benchmark = new ProtoStuffSerializerBenchmark();
        benchmark.setUp();
        ProtoStuffSerializer serializer = new ProtoStuffSerializer();

        IMapFileData record = benchmark.deserializeWalRecord();
        assertFalse(record.isDeleted());
        assertEquals(Long.class.getName(), record.getKeyClassName());
        assertEquals(String.class.getName(), record.getValueClassName());
        assertEquals(1_700_000_000_000L, record.getTimestamp());
        assertEquals(Long.valueOf(1001L), serializer.deserialize(record.getKey(), Long.class));
        String payload = serializer.deserialize(record.getValue(), String.class);
        assertEquals(1024, payload.length());
        for (int index = 0; index < payload.length(); index++) {
            assertEquals((char) ('a' + index % 26), payload.charAt(index));
        }
        for (int iteration = 0; iteration < 10; iteration++) {
            IMapFileData decoded =
                    serializer.deserialize(benchmark.serializeWalRecord(), IMapFileData.class);
            assertEquals(record, decoded);
            assertEquals(record, benchmark.deserializeWalRecord());
        }
    }

    @Test
    void shouldReturnFreshResultsWithoutMutatingTheReusableInputs() {
        ProtoStuffSerializerBenchmark benchmark = new ProtoStuffSerializerBenchmark();
        benchmark.setUp();

        byte[] expectedBytes = benchmark.serializeWalRecord();
        byte[] output = benchmark.serializeWalRecord();
        assertNotSame(expectedBytes, output);
        output[0] ^= 1;
        assertArrayEquals(expectedBytes, benchmark.serializeWalRecord());

        IMapFileData expected = benchmark.deserializeWalRecord();
        IMapFileData decoded = benchmark.deserializeWalRecord();
        assertNotSame(expected, decoded);
        decoded.getKey()[0] ^= 1;
        decoded.getValue()[0] ^= 1;
        decoded.setTimestamp(0);
        assertEquals(expected, benchmark.deserializeWalRecord());
        assertArrayEquals(expectedBytes, benchmark.serializeWalRecord());
    }
}
