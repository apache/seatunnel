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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.catalog;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.type.VarcharTypeInfo;

import java.util.HashMap;

import static com.aliyun.odps.type.TypeInfoFactory.INTERVAL_DAY_TIME;

public class MaxComputeDataTypeConvertorTest {

    private final MaxComputeDataTypeConvertor maxComputeDataTypeConvertor =
            new MaxComputeDataTypeConvertor();

    @Test
    public void testTypeInfoStrToSeaTunnelType() {
        String typeInfoStr = "MAP<STRING,STRING>";
        SeaTunnelDataType<?> seaTunnelType =
                maxComputeDataTypeConvertor.toSeaTunnelType("", typeInfoStr);
        Assertions.assertEquals(BasicType.STRING_TYPE, ((MapType) seaTunnelType).getKeyType());
        Assertions.assertEquals(BasicType.STRING_TYPE, ((MapType) seaTunnelType).getKeyType());
    }

    @Test
    public void testTypeInfoToSeaTunnelType() {
        MapTypeInfo simpleMapTypeInfo =
                TypeInfoFactory.getMapTypeInfo(new VarcharTypeInfo(10), new VarcharTypeInfo(10));
        MapType seaTunnelMapType =
                (MapType) maxComputeDataTypeConvertor.toSeaTunnelType("", simpleMapTypeInfo, null);
        Assertions.assertEquals(BasicType.STRING_TYPE, seaTunnelMapType.getKeyType());
        Assertions.assertEquals(BasicType.STRING_TYPE, seaTunnelMapType.getValueType());
    }

    @Test
    public void testSeaTunnelTypeToTypeInfo() {
        MapType mapType = new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
        MapTypeInfo mapTypeInfo =
                (MapTypeInfo) maxComputeDataTypeConvertor.toConnectorType("", mapType, null);
        Assertions.assertEquals(OdpsType.STRING, mapTypeInfo.getKeyTypeInfo().getOdpsType());
        Assertions.assertEquals(OdpsType.STRING, mapTypeInfo.getValueTypeInfo().getOdpsType());
    }

    @Test
    public void getIdentity() {
        Assertions.assertEquals(
                MaxcomputeConfig.PLUGIN_NAME, maxComputeDataTypeConvertor.getIdentity());
    }

    @Test
    public void testConvertorErrorMsgWithUnsupportedType() {
        SeaTunnelRowType rowType = new SeaTunnelRowType(new String[0], new SeaTunnelDataType[0]);
        MultipleRowType multipleRowType =
                new MultipleRowType(new String[] {"table"}, new SeaTunnelRowType[] {rowType});
        MaxComputeDataTypeConvertor maxCompute = new MaxComputeDataTypeConvertor();
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> maxCompute.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Maxcompute' unsupported convert type 'UNSUPPORTED_TYPE' of 'test' to SeaTunnel data type.]",
                exception.getMessage());
        SeaTunnelRuntimeException exception2 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                maxCompute.toSeaTunnelType(
                                        "test", INTERVAL_DAY_TIME, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Maxcompute' unsupported convert type 'INTERVAL_DAY_TIME' of 'test' to SeaTunnel data type.]",
                exception2.getMessage());
        SeaTunnelRuntimeException exception3 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> maxCompute.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-19], ErrorDescription:['Maxcompute' unsupported convert SeaTunnel data type 'MULTIPLE_ROW' of 'test' to connector data type.]",
                exception3.getMessage());
    }
}
