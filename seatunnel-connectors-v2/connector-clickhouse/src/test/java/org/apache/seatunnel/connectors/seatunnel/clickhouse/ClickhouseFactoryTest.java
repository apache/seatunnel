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

package org.apache.seatunnel.connectors.seatunnel.clickhouse;

import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.ClickhouseSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.ClickhouseSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ClickhouseFactoryTest {
    private static final XXHash64 HASH_INSTANCE = XXHashFactory.fastestInstance().hash64();

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new ClickhouseSourceFactory()).optionRule());
        Assertions.assertNotNull((new ClickhouseSinkFactory()).optionRule());
        Assertions.assertNotNull((new ClickhouseFileSinkFactory()).optionRule());
    }

    @Test
    public void testShared() {
        // Create an instance of the XXHash64 algorithm
        XXHashFactory factory = XXHashFactory.fastestInstance();
        XXHash64 hash64 = factory.hash64();

        // Define your input data
        byte[] input;
        ArrayList<String> strings = new ArrayList<>();

        Map<Long, Long> resultCount = new HashMap<>();
        for (int i = 1; i <= 1000000; i++) {
            input = UUID.randomUUID().toString().getBytes();
            // Calculate the hash value
            long hashValue = hash64.hash(input, 0, input.length, 0);

            // Apply modulo operation to get a non-negative result
            int modulo = 10;
            long nonNegativeResult = (hashValue & Long.MAX_VALUE) % modulo;
            Long keyValue = resultCount.get(nonNegativeResult);

            if (keyValue != null) {
                resultCount.put(nonNegativeResult, keyValue + 1L);

            } else {
                resultCount.put(nonNegativeResult, 1L);
            }
        }
        Long totalResult = 0L;
        for (Long key : resultCount.keySet()) {
            totalResult += resultCount.get(key);
        }
        Assertions.assertEquals(1000000, totalResult);
    }
}
