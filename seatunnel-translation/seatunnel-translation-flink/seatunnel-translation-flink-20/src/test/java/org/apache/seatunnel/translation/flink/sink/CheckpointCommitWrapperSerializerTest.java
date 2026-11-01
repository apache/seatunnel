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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.translation.flink.serialization.CommitWrapperSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

class CheckpointCommitWrapperSerializerTest {
    private final Serializer<String> payload =
            new Serializer<String>() {
                @Override
                public byte[] serialize(String value) {
                    return value.getBytes(StandardCharsets.UTF_8);
                }

                @Override
                public String deserialize(byte[] bytes) {
                    return new String(bytes, StandardCharsets.UTF_8);
                }
            };

    @Test
    void testReadsOriginalCheckpointEnvelope() throws Exception {
        CommitWrapperSerializer<String> original = new CommitWrapperSerializer<>(payload);
        CheckpointCommitWrapperSerializer<String> current =
                new CheckpointCommitWrapperSerializer<>(payload);
        CommitWrapper<String> restored =
                current.deserialize(
                        original.getVersion(), original.serialize(new CommitWrapper<>("prepared")));
        Assertions.assertEquals("prepared", restored.getCommit());
        Assertions.assertFalse(restored instanceof CheckpointCommitWrapper);
    }

    @Test
    void testRoundTripKeepsCheckpointBoundary() throws Exception {
        CheckpointCommitWrapperSerializer<String> current =
                new CheckpointCommitWrapperSerializer<>(payload);
        CommitWrapper<String> restored =
                current.deserialize(
                        current.getVersion(),
                        current.serialize(new CheckpointCommitWrapper<>("prepared", 12345678901L)));
        Assertions.assertEquals("prepared", restored.getCommit());
        Assertions.assertEquals(
                12345678901L, ((CheckpointCommitWrapper<String>) restored).getCheckpointId());
    }

    @Test
    void testRejectsUnknownAndTruncatedEnvelope() {
        CheckpointCommitWrapperSerializer<String> current =
                new CheckpointCommitWrapperSerializer<>(payload);
        Assertions.assertThrows(IOException.class, () -> current.deserialize(9, new byte[0]));
        Assertions.assertThrows(IOException.class, () -> current.deserialize(1, new byte[1]));
        Assertions.assertThrows(
                IOException.class, () -> current.deserialize(0, new byte[] {-1, -1, -1, -1}));
        Assertions.assertThrows(
                IOException.class, () -> current.deserialize(0, new byte[] {0, 0, 0, 10}));
    }
}
