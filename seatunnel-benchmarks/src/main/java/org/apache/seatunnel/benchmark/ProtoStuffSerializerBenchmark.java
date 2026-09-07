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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Threads;

import java.util.concurrent.TimeUnit;

/**
 * Pure in-memory microbenchmarks for the steady-state ProtoStuff WAL record codec.
 *
 * <p>One operation encodes or decodes one IMapFileData envelope, not its nested key/value objects.
 * No filesystem, Hazelcast, or Zeta runtime is started. Thread-local fixtures share the production
 * serializer's static schema cache without sharing mutable input objects. Four threads are used by
 * default to exercise concurrent lookup on the standard four-vCPU runner. IMapFileData is not a
 * serializer wrapper type: both measured methods call getSchema on every invocation, even after
 * setup initializes the schema.
 */
@Threads(4)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
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
public class ProtoStuffSerializerBenchmark extends BenchmarkBase {

    private ProtoStuffSerializer serializer;
    private IMapFileData record;
    private byte[] serializedRecord;

    /** Prepare a fixed WAL record and initialize its schema outside the measured methods. */
    @Setup
    public void setUp() {
        serializer = new ProtoStuffSerializer();
        char[] payload = new char[1024];
        for (int index = 0; index < payload.length; index++) {
            payload[index] = (char) ('a' + index % 26);
        }
        record =
                IMapFileData.builder()
                        .deleted(false)
                        .key(serializer.serialize(1001L))
                        .keyClassName(Long.class.getName())
                        .value(serializer.serialize(new String(payload)))
                        .valueClassName(String.class.getName())
                        .timestamp(1_700_000_000_000L)
                        .build();
        serializedRecord = serializer.serialize(record);
        if (!record.equals(serializer.deserialize(serializedRecord, IMapFileData.class))) {
            throw new IllegalStateException("ProtoStuff WAL record fixture failed to round-trip");
        }
    }

    /** Encode one WAL envelope, including the serializer's normal buffer/output allocations. */
    @Benchmark
    public byte[] serializeWalRecord() {
        return serializer.serialize(record);
    }

    /** Decode one prepared WAL envelope into a newly allocated object. */
    @Benchmark
    public IMapFileData deserializeWalRecord() {
        return serializer.deserialize(serializedRecord, IMapFileData.class);
    }
}
