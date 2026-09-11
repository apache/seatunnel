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

package org.apache.seatunnel.benchmark.storage.imap;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit coverage for WAL-growth accounting that does not start a benchmark environment. */
class IMapWalAppendBenchmarkWorkloadTest {

    @Test
    void calculatesPersistedBytesPerAppend() {
        assertEquals(
                64L, IMapWalAppendBenchmarkWorkload.calculateWalBytesPerAppend(100L, 6_500L, 100L));
    }

    @Test
    void rejectsMissingAppendsOrWalGrowth() {
        assertThrows(
                IllegalStateException.class,
                () -> IMapWalAppendBenchmarkWorkload.calculateWalBytesPerAppend(100L, 200L, 0L));
        assertThrows(
                IllegalStateException.class,
                () -> IMapWalAppendBenchmarkWorkload.calculateWalBytesPerAppend(100L, 100L, 1L));
    }
}
