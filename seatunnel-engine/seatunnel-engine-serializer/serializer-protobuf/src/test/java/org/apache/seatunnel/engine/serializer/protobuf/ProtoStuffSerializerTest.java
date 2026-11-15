/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.serializer.protobuf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ProtoStuffSerializerTest {

    @Test
    public void testConcurrentSchemaInitializationAndReuse() throws Exception {
        int threads = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        ProtoStuffSerializer serializer = new ProtoStuffSerializer();
        List<Future<?>> results = new ArrayList<>();
        try {
            for (int thread = 0; thread < threads; thread++) {
                final int id = thread;
                results.add(
                        executor.submit(
                                () -> {
                                    ready.countDown();
                                    Assertions.assertTrue(start.await(30, TimeUnit.SECONDS));
                                    for (int iteration = 0; iteration < 100; iteration++) {
                                        ConcurrentPayload input = new ConcurrentPayload();
                                        input.id = id;
                                        input.values = Arrays.asList("before", null, "after");
                                        ConcurrentPayload output =
                                                serializer.deserialize(
                                                        serializer.serialize(input),
                                                        ConcurrentPayload.class);
                                        Assertions.assertEquals(input.id, output.id);
                                        Assertions.assertEquals(input.values, output.values);
                                    }
                                    return null;
                                }));
            }
            Assertions.assertTrue(ready.await(30, TimeUnit.SECONDS));
            start.countDown();
            for (Future<?> result : results) {
                result.get(30, TimeUnit.SECONDS);
            }
        } finally {
            start.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    public static class ConcurrentPayload {
        public int id;
        public List<String> values;
    }

    @Test
    public void testProtoStuffSerializerForArrayType() {
        Long[] longs = new Long[10];
        Boolean[] booleans = new Boolean[10];
        Character[] characters = new Character[10];
        Short[] shorts = new Short[10];
        Integer[] integers = new Integer[10];
        Float[] floats = new Float[10];
        Double[] doubles = new Double[10];
        String[] strings = new String[10];

        longs[6] = 111111111L;
        booleans[6] = true;
        characters[6] = 'a';
        shorts[6] = Short.MAX_VALUE;
        integers[6] = 1;
        floats[6] = 1.0f;
        doubles[6] = 1.0;
        strings[6] = "string";

        ProtoStuffSerializer protoStuffSerializer = new ProtoStuffSerializer();
        byte[] serialize1 = protoStuffSerializer.serialize(booleans);
        byte[] serialize3 = protoStuffSerializer.serialize(characters);
        byte[] serialize4 = protoStuffSerializer.serialize(shorts);
        byte[] serialize5 = protoStuffSerializer.serialize(integers);
        byte[] serialize6 = protoStuffSerializer.serialize(floats);
        byte[] serialize7 = protoStuffSerializer.serialize(doubles);
        byte[] serialize8 = protoStuffSerializer.serialize(strings);
        byte[] serialize9 = protoStuffSerializer.serialize(longs);

        Boolean[] deserialize1 = protoStuffSerializer.deserialize(serialize1, Boolean[].class);
        Assertions.assertEquals(deserialize1[6], true);
        Character[] deserialize3 = protoStuffSerializer.deserialize(serialize3, Character[].class);
        Assertions.assertEquals(deserialize3[6], 'a');
        Short[] deserialize4 = protoStuffSerializer.deserialize(serialize4, Short[].class);
        Assertions.assertEquals(deserialize4[6], Short.MAX_VALUE);
        Integer[] deserialize5 = protoStuffSerializer.deserialize(serialize5, Integer[].class);
        Assertions.assertEquals(deserialize5[6], 1);
        Float[] deserialize6 = protoStuffSerializer.deserialize(serialize6, Float[].class);
        Assertions.assertEquals(deserialize6[6], 1.0f);
        Double[] deserialize7 = protoStuffSerializer.deserialize(serialize7, Double[].class);
        Assertions.assertEquals(deserialize7[6], 1.0);
        String[] deserialize8 = protoStuffSerializer.deserialize(serialize8, String[].class);
        Assertions.assertEquals(deserialize8[6], "string");
        Long[] deserialize9 = protoStuffSerializer.deserialize(serialize9, Long[].class);
        Assertions.assertEquals(deserialize9[6], 111111111L);
    }

    @Test
    public void testArrayInit() {

        Long[] arr = new Long[] {1L, null, 2L};
        ProtoStuffSerializer p = new ProtoStuffSerializer();
        byte[] serialize = p.serialize(arr);

        Long[] deserialize = p.deserialize(serialize, Long[].class);
        Assertions.assertEquals(deserialize.length, 3);
        Assertions.assertEquals(deserialize[0], 1L);
        Assertions.assertNull(deserialize[1]);
        Assertions.assertEquals(deserialize[2], 2L);
    }
}
