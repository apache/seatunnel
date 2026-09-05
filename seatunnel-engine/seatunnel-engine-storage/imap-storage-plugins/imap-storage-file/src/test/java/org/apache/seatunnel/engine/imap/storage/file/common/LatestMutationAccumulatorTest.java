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

package org.apache.seatunnel.engine.imap.storage.file.common;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

class LatestMutationAccumulatorTest {

    @Test
    void shouldRetainAtMostOneMutationPerKey() {
        LatestMutationAccumulator accumulator = new LatestMutationAccumulator(null);

        for (int version = 0; version < 100_000; version++) {
            accumulator.accept(mutation("key-" + (version % 100), version, false));
        }

        Assertions.assertEquals(100, accumulator.size());
        Assertions.assertTrue(
                accumulator.values().stream()
                        .allMatch(mutation -> mutation.getTimestamp() >= 99_900));
    }

    @Test
    void shouldChooseNewerMutationRegardlessOfArrivalOrder() {
        LatestMutationAccumulator accumulator = new LatestMutationAccumulator(null);

        accumulator.accept(mutation("key", 20, false));
        accumulator.accept(mutation("key", 10, false));

        Assertions.assertEquals(1, accumulator.size());
        Assertions.assertEquals(20, accumulator.values().iterator().next().getTimestamp());
    }

    private static IMapFileData mutation(String key, long timestamp, boolean deleted) {
        return IMapFileData.builder()
                .key(key.getBytes(StandardCharsets.UTF_8))
                .keyClassName(String.class.getName())
                .value(("value-" + timestamp).getBytes(StandardCharsets.UTF_8))
                .valueClassName(String.class.getName())
                .timestamp(timestamp)
                .deleted(deleted)
                .build();
    }
}
