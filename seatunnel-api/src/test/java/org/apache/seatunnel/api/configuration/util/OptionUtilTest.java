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

package org.apache.seatunnel.api.configuration.util;

import org.apache.seatunnel.api.configuration.Option;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class OptionUtilTest {

    @Test
    public void test() throws InstantiationException, IllegalAccessException {
        List<Option<?>> options = OptionUtil.getOptions(TestOptionConfig.class);
        Assertions.assertEquals("short-value", options.get(0).key());
        Assertions.assertEquals("shortValue", options.get(0).getDescription());
        Assertions.assertEquals(Short.class, options.get(0).typeReference().getType());

        Assertions.assertEquals(Integer.class, options.get(1).typeReference().getType());
        Assertions.assertEquals("int_value", options.get(1).key());
        Assertions.assertEquals("", options.get(1).getDescription());
        Assertions.assertNull(options.get(1).defaultValue());

        Assertions.assertEquals(Long.class, options.get(2).typeReference().getType());

        Assertions.assertEquals(Float.class, options.get(3).typeReference().getType());

        Assertions.assertEquals(Double.class, options.get(4).typeReference().getType());

        Assertions.assertEquals(String.class, options.get(5).typeReference().getType());
        Assertions.assertEquals("default string", options.get(5).defaultValue());

        Assertions.assertEquals(Boolean.class, options.get(6).typeReference().getType());
        Assertions.assertEquals(true, options.get(6).defaultValue());

        Assertions.assertEquals(Byte.class, options.get(7).typeReference().getType());

        Assertions.assertEquals(Character.class, options.get(8).typeReference().getType());
        Assertions.assertEquals(TestOptionConfigEnum.class, options.get(9).typeReference().getType());
        Assertions.assertEquals(TestOptionConfigEnum.KEY2, options.get(9).defaultValue());

        Assertions.assertEquals(TestOptionConfig.class, options.get(10).typeReference().getType());

        Assertions.assertEquals(List.class, options.get(11).typeReference().getType());

        Assertions.assertEquals(Map.class, options.get(12).typeReference().getType());

    }

}
