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

import java.nio.charset.StandardCharsets;

class RecoveryCommitSerializerTest {
    private final CommitWrapperSerializer<String> legacy =
            new CommitWrapperSerializer<>(
                    new Serializer<String>() {
                        public byte[] serialize(String value) {
                            return value.getBytes(StandardCharsets.UTF_8);
                        }

                        public String deserialize(byte[] value) {
                            return new String(value, StandardCharsets.UTF_8);
                        }
                    });

    @Test
    void testOldStateAndLocalRestoreCutoff() throws Exception {
        RecoveredCommitTracker tracker = new RecoveredCommitTracker();
        RecoveryCommitSerializer<String> serializer =
                new RecoveryCommitSerializer<>(legacy, tracker);
        CommitWrapper<String> saved =
                serializer.deserialize(0, legacy.serialize(new CommitWrapper<>("same")));
        tracker.finishRestore();
        Assertions.assertTrue(saved.isRecovered());
        CommitWrapper<String> live =
                serializer.deserialize(0, serializer.serialize(new CommitWrapper<>("same")));
        Assertions.assertFalse(live.isRecovered());
        Assertions.assertEquals(saved.getCommit(), live.getCommit());
    }

    @Test
    void testFormatIsUnchangedAndNetworkReceiverClassifiesLineage() throws Exception {
        RecoveredCommitTracker tracker = new RecoveredCommitTracker();
        tracker.finishRestore();
        RecoveryCommitSerializer<String> serializer =
                new RecoveryCommitSerializer<>(legacy, tracker);
        CommitWrapper<String> replay = new CommitWrapper<>("prepared");
        replay.markRecovered();
        Assertions.assertEquals(legacy.getVersion(), serializer.getVersion());
        Assertions.assertArrayEquals(legacy.serialize(replay), serializer.serialize(replay));
        CommitWrapper<String> received = serializer.deserialize(0, serializer.serialize(replay));
        Assertions.assertFalse(received.isRecovered());
        RestoredCommittableOperator.markRecovered(
                new org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage<>(
                        received, 7L, 0),
                7);
        Assertions.assertTrue(received.isRecovered());
    }
}
