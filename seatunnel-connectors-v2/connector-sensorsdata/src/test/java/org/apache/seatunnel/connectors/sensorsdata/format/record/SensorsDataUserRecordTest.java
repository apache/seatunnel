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
class SensorsDataUserRecordTest {

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
        row = new SeaTunnelRow(values);
    }

    @Test
    public void testUserRecord() {
        try {
            ReadonlyConfig readonlyConfig =
                    ReadonlyConfig.fromMap(
                            ImmutableMap.<String, Object>builder()
                                    .put("record_type", "users")
                                    .put("distinct_id_column", "name")
                                    .put(
                                            "identity_fields",
                                            Arrays.asList(
                                                    new TargetColumnConfig(
                                                            "name", "String", "$identity_name"),
                                                    new TargetColumnConfig(
                                                            "list_col1",
                                                            "List",
                                                            "$identity_distinct_id")))
                                    .put(
                                            "property_fields",
                                            Arrays.asList(
                                                    new TargetColumnConfig("name", "String"),
                                                    new TargetColumnConfig(
                                                            "str_col", "String", "str_prop"),
                                                    new TargetColumnConfig(
                                                            "list_col2", "LIST_COMMA", "list_prop"),
                                                    new TargetColumnConfig(
                                                            "double_col", "DOUBLE", "double_prop")))
                                    .build());
            SensorsDataConfigBase config = new SensorsDataConfigBase(readonlyConfig);
            RowAccessor ra = new RowAccessor(config, rowType);
            UserRecordBase record = UserRecordBase.newBuilder(ra).buildUserRecord(row);
            String json = record.toJsonString();
            log.info("UserRecord: " + json);
            try {
                ObjectNode node = (ObjectNode) OBJECT_MAPPER.readTree(json);
                node.remove("_track_id");
                node.remove("time");
                Assertions.assertEquals(
                        "{\"schema\":\"users\",\"identities\":{\"$identity_distinct_id\":[\"abc\",\"bcd\",\"cdb\"],\"$identity_name\":[\"abc\"]},\"lib\":{\"$lib\":\"Java\",\"$lib_version\":\"3.6.9\",\"$lib_method\":\"code\",\"$lib_detail\":\"JavaSDK##generateLibInfo\"},\"distinct_id\":\"abc\",\"type\":\"profile_set\",\"version\":\"2.0\",\"properties\":{\"str_prop\":\"abc\",\"double_prop\":123.12,\"name\":\"abc\",\"list_prop\":[\"abc\",\"bcd\",\"cdb\"]}}",
                        OBJECT_MAPPER.writeValueAsString(node));
            } catch (JsonProcessingException e) {
                Assertions.fail(e.getMessage());
            }
        } catch (Exception e) {
            log.error("fail", e);
        }
    }

    @Test
    public void testEventRecord1() {
        ReadonlyConfig readonlyConfig =
                ReadonlyConfig.fromMap(
                        ImmutableMap.<String, Object>builder()
                                .put("record_type", "events")
                                .put("distinct_id_column", "name")
                                .put("event_name", "${str_col}")
                                .put("time_column", "time_int_col")
                                .put(
                                        "identity_fields",
                                        Arrays.asList(
                                                new TargetColumnConfig(
                                                        "str_col", "String", "$identity_login_id"),
                                                new TargetColumnConfig(
                                                        "name", "string", "$identity_name")))
                                .put(
                                        "property_fields",
                                        Arrays.asList(
                                                new TargetColumnConfig("name", "string"),
                                                new TargetColumnConfig(
                                                        "str_col", "string", "str_prop"),
                                                new TargetColumnConfig(
                                                        "list_col2", "LIST_COMMA", "list_prop")))
                                .build());
        SensorsDataConfigBase config = new SensorsDataConfigBase(readonlyConfig);
        RowAccessor ra = new RowAccessor(config, rowType);
        String json = SensorsDataRecordBuilder.newBuilder(config, ra).build(row).toJsonString();
        log.info("UserEventRecord1: " + json);
        try {
            ObjectNode node = (ObjectNode) OBJECT_MAPPER.readTree(json);
            node.remove("_track_id");
            Assertions.assertEquals(
                    "{\"schema\":\"events\",\"lib\":{\"$lib\":\"Java\",\"$lib_version\":\"3.6.9\",\"$lib_method\":\"code\",\"$lib_detail\":\"JavaSDK##generateLibInfo\"},\"time\":1711423014152,\"type\":\"track\",\"event\":\"abc\",\"version\":\"2.0\",\"properties\":{\"str_prop\":\"abc\",\"$time\":1711423014152,\"name\":\"abc\",\"identities\":{\"$identity_name\":\"abc\",\"$identity_login_id\":\"abc\"},\"list_prop\":[\"abc\",\"bcd\",\"cdb\"],\"distinct_id\":\"abc\"}}",
                    OBJECT_MAPPER.writeValueAsString(node));
        } catch (JsonProcessingException e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testEventRecord2() {
        ReadonlyConfig readonlyConfig =
                ReadonlyConfig.fromMap(
                        ImmutableMap.<String, Object>builder()
                                .put("record_type", "events")
                                .put("distinct_id_column", "name")
                                .put("event_name", "$AppStart")
                                .put("time_column", "time_int_col")
                                .put(
                                        "identity_fields",
                                        Arrays.asList(
                                                new TargetColumnConfig(
                                                        "str_col", "String", "$identity_login_id"),
                                                new TargetColumnConfig(
                                                        "name", "string", "$identity_name")))
                                .put(
                                        "property_fields",
                                        Arrays.asList(
                                                new TargetColumnConfig(
                                                        "str_col", "string", "$project"),
                                                new TargetColumnConfig(
                                                        "str_col", "string", "$token"),
                                                new TargetColumnConfig("name", "string"),
                                                new TargetColumnConfig(
                                                        "str_col", "string", "str_prop"),
                                                new TargetColumnConfig(
                                                        "list_col2", "LIST_COMMA", "list_prop")))
                                .build());
        SensorsDataConfigBase config = new SensorsDataConfigBase(readonlyConfig);
        RowAccessor ra = new RowAccessor(config, rowType);
        String json = SensorsDataRecordBuilder.newBuilder(config, ra).build(row).toJsonString();
        log.info("UserEventRecord2: " + json);
        try {
            ObjectNode node = (ObjectNode) OBJECT_MAPPER.readTree(json);
            node.remove("_track_id");
            Assertions.assertEquals(
                    "{\"schema\":\"events\",\"lib\":{\"$lib\":\"Java\",\"$lib_version\":\"3.6.9\",\"$lib_method\":\"code\",\"$lib_detail\":\"JavaSDK##generateLibInfo\"},\"project\":\"abc\",\"time\":1711423014152,\"type\":\"track\",\"event\":\"$AppStart\",\"version\":\"2.0\",\"properties\":{\"$token\":\"abc\",\"str_prop\":\"abc\",\"$time\":1711423014152,\"identities\":{\"$identity_name\":\"abc\",\"$identity_login_id\":\"abc\"},\"distinct_id\":\"abc\",\"name\":\"abc\",\"list_prop\":[\"abc\",\"bcd\",\"cdb\"],\"$project\":\"abc\"},\"token\":\"abc\"}",
                    OBJECT_MAPPER.writeValueAsString(node));
        } catch (JsonProcessingException e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testDetailRecord1() {
        ReadonlyConfig readonlyConfig =
                ReadonlyConfig.fromMap(
                        ImmutableMap.<String, Object>builder()
                                .put("record_type", "details")
                                .put("schema", "s_order")
                                .put("distinct_id_column", "name")
                                .put("detail_id_column", "name")
                                .put(
                                        "identity_fields",
                                        Arrays.asList(
                                                new TargetColumnConfig(
                                                        "str_col", "String", "$identity_login_id"),
                                                new TargetColumnConfig(
                                                        "name", "string", "$identity_name")))
                                .put(
                                        "property_fields",
                                        Arrays.asList(
                                                new TargetColumnConfig("name", "string"),
                                                new TargetColumnConfig(
                                                        "str_col", "string", "str_prop"),
                                                new TargetColumnConfig(
                                                        "list_col2", "LIST_COMMA", "list_prop")))
                                .build());
        SensorsDataConfigBase config = new SensorsDataConfigBase(readonlyConfig);
        RowAccessor ra = new RowAccessor(config, rowType);
        String json = SensorsDataRecordBuilder.newBuilder(config, ra).build(row).toJsonString();
        log.info("UserDetailRecord: " + json);
        try {
            ObjectNode node = (ObjectNode) OBJECT_MAPPER.readTree(json);
            node.remove("_track_id");
            node.remove("time");
            Assertions.assertEquals(
                    "{\"schema\":\"s_order\",\"lib\":{\"$lib\":\"Java\",\"$lib_version\":\"3.6.9\",\"$lib_method\":\"code\",\"$lib_detail\":\"JavaSDK##generateLibInfo\"},\"id\":\"abc\",\"type\":\"detail_set\",\"version\":\"2.0\",\"properties\":{\"str_prop\":\"abc\",\"name\":\"abc\",\"identities\":{\"$identity_name\":\"abc\",\"$identity_login_id\":\"abc\"},\"list_prop\":[\"abc\",\"bcd\",\"cdb\"],\"distinct_id\":\"abc\"}}",
                    OBJECT_MAPPER.writeValueAsString(node));
        } catch (JsonProcessingException e) {
            Assertions.fail(e.getMessage());
        }
    }
}
