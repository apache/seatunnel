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

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SeaTunnelDataTypeConvertorUtilTest {

    @Test
    void testParseWithUnsupportedType() {
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                        "test", "MULTIPLE_ROW"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:['SeaTunnel' unsupported data type 'MULTIPLE_ROW' of 'test']",
                exception.getMessage());

        SeaTunnelRuntimeException exception2 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                        "test", "map<string, MULTIPLE_ROW>"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:['SeaTunnel' unsupported data type 'MULTIPLE_ROW' of 'test']",
                exception2.getMessage());

        SeaTunnelRuntimeException exception3 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                        "test", "array<MULTIPLE_ROW>"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:['SeaTunnel' unsupported data type 'MULTIPLE_ROW' of 'test']",
                exception3.getMessage());

        SeaTunnelRuntimeException exception4 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                        "test", "uuid"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:['SeaTunnel' unsupported data type 'uuid' of 'test']",
                exception4.getMessage());

        IllegalArgumentException exception5 =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                        "test", "{uuid}"));
        String expectedMsg5 =
                String.format("HOCON Config parse from %s failed.", "{conf = {uuid}}");
        Assertions.assertEquals(expectedMsg5, exception5.getMessage());

        String invalidTypeDeclaration = "[e]";
        IllegalArgumentException exception6 =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                        "test",
                                        String.format("{c_0 = %s}", invalidTypeDeclaration)));
        String expectedMsg6 =
                String.format(
                        "Unsupported parse SeaTunnel Type from '%s'.", invalidTypeDeclaration);
        Assertions.assertEquals(expectedMsg6, exception6.getMessage());
    }
}
