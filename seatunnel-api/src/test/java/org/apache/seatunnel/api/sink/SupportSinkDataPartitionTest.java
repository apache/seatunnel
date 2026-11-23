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

package org.apache.seatunnel.api.sink;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SupportSinkDataPartitionTest {

    private static final SinkDataPartitioner<String> ROUTING =
            new SinkDataPartitioner<String>() {
                @Override
                public int select(String record) {
                    return record.length() % 2;
                }

                @Override
                public Optional<String> targetIdentifier() {
                    return Optional.of("test-target");
                }
            };

    @Test
    void shouldLeavePlainSinkUnrouted() {
        assertFalse(SupportSinkDataPartition.resolve(new PlainSink(), 2).isPresent());
    }

    @Test
    void shouldResolveRoutingCapability() {
        RoutingSink sink = new RoutingSink(Optional.of(ROUTING));
        SinkDataPartitioner<String> routing = SupportSinkDataPartition.resolve(sink, 2).get();
        assertSame(ROUTING, routing);
        assertEquals(1, routing.select("abc"));
    }

    @Test
    void shouldBindWriterCountWhenBuildingRouting() {
        RoutingSink sink =
                new RoutingSink(Optional.empty()) {
                    @Override
                    public Optional<SinkDataPartitioner<String>> getSinkDataPartitioner(
                            int writerCount) {
                        return Optional.of(
                                new SinkDataPartitioner<String>() {
                                    @Override
                                    public int select(String record) {
                                        return record.length() % writerCount;
                                    }

                                    @Override
                                    public Optional<String> targetIdentifier() {
                                        return Optional.of("bound-target");
                                    }
                                });
                    }
                };
        SinkDataPartitioner<String> twoWriters = SupportSinkDataPartition.resolve(sink, 2).get();
        SinkDataPartitioner<String> fourWriters = SupportSinkDataPartition.resolve(sink, 4).get();
        assertEquals(1, twoWriters.select("abc"));
        assertEquals(3, fourWriters.select("abc"));
        assertEquals(1, twoWriters.select("abc"));
    }

    @Test
    void shouldAllowCapabilityToDisableRouting() {
        assertFalse(
                SupportSinkDataPartition.resolve(new RoutingSink(Optional.empty()), 2).isPresent());
    }

    @Test
    void shouldResolveRoutingAfterSinkConfigurationChanges() {
        RoutingSink sink = new RoutingSink(Optional.empty());
        assertFalse(SupportSinkDataPartition.resolve(sink, 2).isPresent());
        sink.routing = Optional.of(ROUTING);
        assertSame(ROUTING, SupportSinkDataPartition.resolve(sink, 2).get());
    }

    @Test
    void shouldRejectNonPositiveWriterCount() {
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> SupportSinkDataPartition.resolve(new PlainSink(), 0));
        assertEquals("Sink writer parallelism must be positive", failure.getMessage());
    }

    @Test
    void shouldPropagateCapabilityValidationFailure() {
        IllegalStateException failure = new IllegalStateException("invalid routing configuration");
        RoutingSink sink =
                new RoutingSink(Optional.empty()) {
                    @Override
                    public Optional<SinkDataPartitioner<String>> getSinkDataPartitioner(
                            int writerCount) {
                        throw failure;
                    }
                };
        assertSame(
                failure,
                assertThrows(
                        IllegalStateException.class,
                        () -> SupportSinkDataPartition.resolve(sink, 2)));
    }

    private static class PlainSink implements SeaTunnelSink<String, Void, Void, Void> {

        @Override
        public String getPluginName() {
            return "test-sink";
        }

        @Override
        public SinkWriter<String, Void, Void> createWriter(SinkWriter.Context context) {
            throw new UnsupportedOperationException("This fixture only provides sink routing");
        }
    }

    private static class RoutingSink extends PlainSink implements SupportSinkDataPartition<String> {

        private Optional<SinkDataPartitioner<String>> routing;

        private RoutingSink(Optional<SinkDataPartitioner<String>> routing) {
            this.routing = routing;
        }

        @Override
        public Optional<SinkDataPartitioner<String>> getSinkDataPartitioner(int writerCount) {
            return routing;
        }
    }
}
