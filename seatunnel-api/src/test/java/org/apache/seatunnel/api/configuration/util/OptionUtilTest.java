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
        Assertions.assertEquals(options.get(0).key(), "short-value");
        Assertions.assertEquals(options.get(0).getDescription(), "shortValue");
        Assertions.assertEquals(options.get(0).typeReference().getType(), Short.class);

        Assertions.assertEquals(options.get(1).typeReference().getType(), Integer.class);
        Assertions.assertEquals(options.get(1).key(), "int_value");
        Assertions.assertEquals(options.get(1).getDescription(), "");
        Assertions.assertNull(options.get(1).defaultValue());

        Assertions.assertEquals(options.get(2).typeReference().getType(), Long.class);

        Assertions.assertEquals(options.get(3).typeReference().getType(), Float.class);

        Assertions.assertEquals(options.get(4).typeReference().getType(), Double.class);

        Assertions.assertEquals(options.get(5).typeReference().getType(), String.class);
        Assertions.assertEquals(options.get(5).defaultValue(), "default string");

        Assertions.assertEquals(options.get(6).typeReference().getType(), Boolean.class);
        Assertions.assertEquals(options.get(6).defaultValue(), true);

        Assertions.assertEquals(options.get(7).typeReference().getType(), Byte.class);

        Assertions.assertEquals(options.get(8).typeReference().getType(), Character.class);
        Assertions.assertEquals(options.get(9).typeReference().getType(), TestOptionConfigEnum.class);
        Assertions.assertEquals(options.get(9).defaultValue(), TestOptionConfigEnum.KEY2);

        Assertions.assertEquals(options.get(10).typeReference().getType(), TestOptionConfig.class);

        Assertions.assertEquals(options.get(11).typeReference().getType(), List.class);

        Assertions.assertEquals(options.get(12).typeReference().getType(), Map.class);

    }

}
