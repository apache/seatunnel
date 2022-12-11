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
    void testProtoStuffSerializerForArrayType() {
        System.setProperty("protostuff.runtime.preserve_null_elements", "true");
        Long[] data = new Long[10];
        data[6] = 111111111L;
        ProtoStuffSerializer protoStuffSerializer = new ProtoStuffSerializer();
        byte[] serialize = protoStuffSerializer.serialize(data);
        Long[] deserialize = protoStuffSerializer.deserialize(serialize, Long[].class);
        Assertions.assertEquals(deserialize[6], 111111111L);
    }
}
