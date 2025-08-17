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

package org.apache.seatunnel.connectors.sensorsdata.format.record;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.google.common.collect.ImmutableMap;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.sensorsdata.format.config.SensorsDataConfigBase;
import org.apache.seatunnel.connectors.sensorsdata.format.config.TargetColumnConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

@Slf4j
class SensorsDataSpecialItemRecordTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private SeaTunnelRowType rowType;
    private SeaTunnelRow row;

    @BeforeEach
    public void setUp() {
        rowType =
                new SeaTunnelRowType(
                        new String[] {
                            "id",
                            "name",
                            "int_col",
                            "bigint_col",
                            "double_col",
                            "float_col",
                            "str_col",
                            "list_col1",
                            "list_col2",
                            "list_col3",
                            "time_int_col",
                            "time_str_col1",
                            "time_str_col2",
                            "bool_col",
                            "item_id_col",
                            "item_type_col",
                            "project_col",
                            "token_col",
                        },
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.DOUBLE_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                        });
        Object[] values = new Object[rowType.getFieldNames().length];
        values[0] = 123;
        values[1] = "abc";
        values[2] = 3;
        values[3] = 1711423014152L;
        values[4] = 123.12;
        values[5] = (float) 2.2;
        values[6] = "abc";
        values[7] = "abc\nbcd\ncdb";
        values[8] = "abc,bcd,cdb";
        values[9] = "abc;bcd;cdb";
        values[10] = 1711423014152L;
        values[11] = "1711423014152";
        values[12] = "2024-03-26 11:31:12";
        values[13] = true;
        values[14] = "123";
        values[15] = "items";
        values[16] = "production";
        values[17] = "12345678";
        row = new SeaTunnelRow(values);
    }

    @Test
    public void testUserRecord() {
        try {
            ReadonlyConfig readonlyConfig =
                    ReadonlyConfig.fromMap(
                            ImmutableMap.<String, Object>builder()
                                    .put("entity_name", "items")
                                    .put("record_type", "items")
                                    .put("item_id_column", "item_id_col")
                                    .put("item_type_column", "item_type_col")
                                    .put(
                                            "property_fields",
                                            Arrays.asList(
                                                    new TargetColumnConfig("name", "String"),
                                                    new TargetColumnConfig(
                                                            "str_col", "String", "str_prop"),
                                                    new TargetColumnConfig(
                                                            "list_col2", "LIST_COMMA", "list_prop"),
                                                    new TargetColumnConfig(
                                                            "double_col", "DOUBLE", "double_prop"),
                                                    new TargetColumnConfig(
                                                            "time_str_col1", "DOUBLE", "date_prop"),
                                                    new TargetColumnConfig(
                                                            "bool_col", "BOOLEAN", "bool_prop"),
                                                    new TargetColumnConfig(
                                                            "token_col", "STRING", "$token"),
                                                    new TargetColumnConfig(
                                                            "project_col", "STRING", "$project")))
                                    .build());
            SensorsDataConfigBase config = new SensorsDataConfigBase(readonlyConfig);
            RowAccessor ra = new RowAccessor(config, rowType);
            SensorsDataRecord record = SensorsDataRecordBuilder.newBuilder(config, ra).build(row);
            String json = record.toJsonString();
            log.info("ItemRecord: " + json);
            try {
                ObjectNode node = (ObjectNode) OBJECT_MAPPER.readTree(json);
                node.remove("_track_id");
                node.remove("time");
                Assertions.assertEquals(
                        "{\"lib\":{\"$lib\":\"Java\",\"$lib_version\":\"3.6.9\",\"$lib_method\":\"code\",\"$lib_detail\":\"JavaSDK##generateLibInfo\"},\"item_id\":\"123\",\"item_type\":\"items\",\"project\":\"production\",\"type\":\"item_set\",\"properties\":{\"str_prop\":\"abc\",\"double_prop\":123.12,\"name\":\"abc\",\"bool_prop\":true,\"list_prop\":[\"abc\",\"bcd\",\"cdb\"],\"date_prop\":1711423014152},\"token\":\"12345678\"}",
                        OBJECT_MAPPER.writeValueAsString(node));
            } catch (JsonProcessingException e) {
                Assertions.fail(e.getMessage());
            }
        } catch (Exception e) {
            log.error("fail", e);
            Assertions.fail(e.getMessage());
        }
    }
}
