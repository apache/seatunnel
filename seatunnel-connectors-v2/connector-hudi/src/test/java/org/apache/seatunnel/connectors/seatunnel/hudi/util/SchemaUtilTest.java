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

package org.apache.seatunnel.connectors.seatunnel.hudi.util;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SchemaUtilTest {

    @Test
    public void shouldConvertMapWithStringValues() {
        Schema schema = SchemaBuilder.map().values().stringType();
        SeaTunnelDataType<?> dataType = SchemaUtil.convertSeaTunnelType("map_col", schema);
        Assertions.assertEquals(
                new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE), dataType);
    }

    @Test
    public void shouldConvertMapWithIntValues() {
        Schema schema = SchemaBuilder.map().values().intType();
        SeaTunnelDataType<?> dataType = SchemaUtil.convertSeaTunnelType("map_col", schema);
        Assertions.assertEquals(new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE), dataType);
    }

    @Test
    public void shouldConvertMapWithNullableValues() {
        Schema schema =
                SchemaBuilder.map().values().unionOf().nullType().and().longType().endUnion();
        SeaTunnelDataType<?> dataType = SchemaUtil.convertSeaTunnelType("map_col", schema);
        Assertions.assertEquals(
                new MapType<>(BasicType.STRING_TYPE, BasicType.LONG_TYPE), dataType);
    }

    @Test
    public void shouldConvertMapWithArrayValues() {
        Schema schema = SchemaBuilder.map().values().array().items().doubleType();
        SeaTunnelDataType<?> dataType = SchemaUtil.convertSeaTunnelType("map_col", schema);
        Assertions.assertEquals(
                new MapType<>(BasicType.STRING_TYPE, ArrayType.DOUBLE_ARRAY_TYPE), dataType);
    }

    @Test
    public void shouldConvertMapWithDecimalValues() {
        Schema decimal = LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES));
        Schema schema = Schema.createMap(decimal);
        SeaTunnelDataType<?> dataType = SchemaUtil.convertSeaTunnelType("map_col", schema);
        Assertions.assertEquals(
                new MapType<>(BasicType.STRING_TYPE, new DecimalType(10, 2)), dataType);
    }

    @Test
    public void shouldConvertRecordContainingMapColumn() {
        Schema schema =
                SchemaBuilder.record("test_record")
                        .fields()
                        .name("id")
                        .type()
                        .longType()
                        .noDefault()
                        .name("attributes")
                        .type()
                        .map()
                        .values()
                        .stringType()
                        .noDefault()
                        .endRecord();
        SeaTunnelRowType rowType =
                (SeaTunnelRowType) SchemaUtil.convertSeaTunnelType("record", schema);
        Assertions.assertEquals(2, rowType.getTotalFields());
        Assertions.assertEquals("id", rowType.getFieldName(0));
        Assertions.assertEquals(BasicType.LONG_TYPE, rowType.getFieldType(0));
        Assertions.assertEquals("attributes", rowType.getFieldName(1));
        Assertions.assertEquals(
                new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                rowType.getFieldType(1));
    }

    @Test
    public void shouldStillConvertArrays() {
        Schema schema = SchemaBuilder.array().items().stringType();
        SeaTunnelDataType<?> dataType = SchemaUtil.convertSeaTunnelType("array_col", schema);
        Assertions.assertEquals(ArrayType.STRING_ARRAY_TYPE, dataType);
    }
}
