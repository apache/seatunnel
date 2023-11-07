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

package org.apache.seatunnel.api.table.catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SeaTunnelDataTypeConvertorUtilTest {

    @Test
    void testParseWithUnsupportedType() {

        UnsupportedOperationException exception =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () -> SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType("uuid"));
        Assertions.assertEquals("the type[uuid] is not support", exception.getMessage());

        RuntimeException exception2 =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () ->
                                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                        "{uuid}"));
        Assertions.assertEquals(
                "String json deserialization exception.{uuid}", exception2.getMessage());
    }
}
