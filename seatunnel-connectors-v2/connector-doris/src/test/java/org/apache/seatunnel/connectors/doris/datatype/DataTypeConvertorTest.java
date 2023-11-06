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

package org.apache.seatunnel.connectors.doris.datatype;

import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class DataTypeConvertorTest {

    @Test
    void testConvertorErrorMsgWithUnsupportedType() {
        SeaTunnelRowType rowType = new SeaTunnelRowType(new String[0], new SeaTunnelDataType[0]);
        MultipleRowType multipleRowType =
                new MultipleRowType(new String[] {"table"}, new SeaTunnelRowType[] {rowType});
        DorisDataTypeConvertor doris = new DorisDataTypeConvertor();
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> doris.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Doris' unsupported convert type 'UNSUPPORTED_TYPE' of 'test' to SeaTunnel data type.]",
                exception.getMessage());
        SeaTunnelRuntimeException exception2 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> doris.toSeaTunnelType("test", "UNSUPPORTED_TYPE", new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Doris' unsupported convert type 'UNSUPPORTED_TYPE' of 'test' to SeaTunnel data type.]",
                exception2.getMessage());
        SeaTunnelRuntimeException exception3 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> doris.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-19], ErrorDescription:['Doris' unsupported convert SeaTunnel data type 'MULTIPLE_ROW' of 'test' to connector data type.]",
                exception3.getMessage());
    }
}
