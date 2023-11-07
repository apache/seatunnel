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

package org.apache.seatunnel.connectors.seatunnel.starrocks.catalog;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static com.mysql.cj.MysqlType.UNKNOWN;

public class DataTypeConvertorTest {

    @Test
    void testConvertorErrorMsgWithUnsupportedType() {
        SeaTunnelRowType rowType = new SeaTunnelRowType(new String[0], new SeaTunnelDataType[0]);
        MultipleRowType multipleRowType =
                new MultipleRowType(new String[] {"table"}, new SeaTunnelRowType[] {rowType});
        StarRocksDataTypeConvertor starrocks = new StarRocksDataTypeConvertor();
        DataTypeConvertException exception =
                Assertions.assertThrows(
                        DataTypeConvertException.class,
                        () -> starrocks.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:[Unsupported data type] - Convert type: UNKNOWN of the test field to SeaTunnel data type error.",
                exception.getMessage());
        DataTypeConvertException exception2 =
                Assertions.assertThrows(
                        DataTypeConvertException.class,
                        () -> starrocks.toSeaTunnelType("test", UNKNOWN, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:[Unsupported data type] - Convert type: UNKNOWN of the test field to SeaTunnel data type error.",
                exception2.getMessage());
        StarRocksConnectorException exception3 =
                Assertions.assertThrows(
                        StarRocksConnectorException.class,
                        () -> starrocks.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:[Unsupported data type] - Doris doesn't support type 'MULTIPLE_ROW' of the 'test' field yet",
                exception3.getMessage());
    }
}
