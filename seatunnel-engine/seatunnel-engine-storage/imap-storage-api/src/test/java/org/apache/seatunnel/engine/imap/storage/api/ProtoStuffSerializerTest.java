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

package org.apache.seatunnel.engine.imap.storage.api;

import org.apache.seatunnel.engine.imap.storage.api.common.ProtoStuffSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProtoStuffSerializerTest {

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

        Long[] arr = new Long[]{1L, null, 2L};
        ProtoStuffSerializer p = new ProtoStuffSerializer();
        byte[] serialize = p.serialize(arr);

        Long[] deserialize = p.deserialize(serialize, Long[].class);
        Assertions.assertEquals(deserialize.length, 3);
        Assertions.assertEquals(deserialize[0], 1L);
        Assertions.assertNull(deserialize[1]);
        Assertions.assertEquals(deserialize[2], 2L);
    }
}
