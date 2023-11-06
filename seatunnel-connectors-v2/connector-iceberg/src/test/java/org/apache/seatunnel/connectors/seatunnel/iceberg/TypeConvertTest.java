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

package org.apache.seatunnel.connectors.seatunnel.iceberg;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.IcebergTypeMapper;

import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TypeConvertTest {

    @Test
    void testWithUnsupportedType() {
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> IcebergTypeMapper.mapping("test", new Types.UUIDType()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Iceberg' unsupported convert type 'uuid' of 'test' to SeaTunnel data type.]",
                exception.getMessage());

        SeaTunnelRuntimeException exception2 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                IcebergTypeMapper.mapping(
                                        "test",
                                        Types.StructType.of(
                                                Types.NestedField.of(
                                                        1, false, "key", new Types.UUIDType()),
                                                Types.NestedField.of(
                                                        2, false, "value", new Types.UUIDType()))));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Iceberg' unsupported convert type 'uuid' of 'key' to SeaTunnel data type.]",
                exception2.getMessage());

        SeaTunnelRuntimeException exception3 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                IcebergTypeMapper.mapping(
                                        "test",
                                        Types.MapType.ofOptional(
                                                1, 1, new Types.UUIDType(), new Types.UUIDType())));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Iceberg' unsupported convert type 'uuid' of 'test' to SeaTunnel data type.]",
                exception3.getMessage());
    }
}
