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

package org.apache.seatunnel.benchmark.connector.sink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LatencyHistogramTest {

    @Test
    void shouldKeepPercentileExactWhenOverflowIsAboveItsRank() {
        LatencyHistogram histogram = new LatencyHistogram(10);
        for (int index = 0; index < 99; index++) {
            histogram.record(5L);
        }
        histogram.record(11L);

        PercentileResult result = histogram.percentile(0.99D);

        assertEquals(5L, result.getValueMillis());
        assertFalse(result.isClamped());
    }

    @Test
    void shouldMarkPercentileInOverflowBucketAsClamped() {
        LatencyHistogram histogram = new LatencyHistogram(10);
        for (int index = 0; index < 98; index++) {
            histogram.record(5L);
        }
        histogram.record(11L);
        histogram.record(12L);

        PercentileResult result = histogram.percentile(0.99D);

        assertEquals(11L, result.getValueMillis());
        assertTrue(result.isClamped());
        assertEquals(12L, histogram.getMaximum());
    }

    @Test
    void shouldReturnUnclampedZeroForEmptyHistogram() {
        PercentileResult result = new LatencyHistogram(10).percentile(0.99D);

        assertEquals(0L, result.getValueMillis());
        assertFalse(result.isClamped());
    }
}
