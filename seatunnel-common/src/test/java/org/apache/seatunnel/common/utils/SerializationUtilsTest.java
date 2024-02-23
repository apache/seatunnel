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

package org.apache.seatunnel.common.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;

public class SerializationUtilsTest {

    @Test
    public void testObjectToString() {

        HashMap<String, String> data = new HashMap<>();
        data.put("key1", "value1");
        data.put("seatunnelTest", "apache SeaTunnel");
        data.put("中 文", "Apache Asia");
        String configStr = SerializationUtils.objectToString(data);
        Assertions.assertNotNull(configStr);

        HashMap<String, String> dataAfter = SerializationUtils.stringToObject(configStr);

        Assertions.assertEquals(dataAfter, data);

        data.put("key2", "");
        Assertions.assertNotEquals(dataAfter, data);
    }

    @Test
    public void testByteToObject() {

        HashMap<String, String> data = new HashMap<>();
        data.put("key1", "value1");
        data.put("seatunnelTest", "apache SeaTunnel");
        data.put("中 文", "Apache Asia");

        ArrayList<HashMap<String, String>> array = new ArrayList<>();
        array.add(data);
        HashMap<String, String> data2 = new HashMap<>();
        data2.put("Apache Asia", "中 文");
        data2.put("value1", "key1");
        data2.put("apache SeaTunnel", "seatunnelTest");
        array.add(data2);

        byte[] result = SerializationUtils.serialize(array);

        ArrayList<HashMap<String, String>> array2 = SerializationUtils.deserialize(result);

        Assertions.assertEquals(array2, array);

        Assertions.assertThrows(
                SerializationException.class,
                () -> SerializationUtils.deserialize(new byte[] {1, 0, 1}));
    }
}
