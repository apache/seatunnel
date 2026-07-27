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

package org.apache.seatunnel.connectors.bigquery.schema;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BigQueryTypeConverterTest {

    @Test
    void testScalarAndCompositeDdlTypes() {
        assertEquals("INT64", BigQueryTypeConverter.toDdlType(BasicType.LONG_TYPE));
        assertEquals("NUMERIC(30, 8)", BigQueryTypeConverter.toDdlType(new DecimalType(30, 8)));
        assertEquals(
                "BIGNUMERIC(50, 10)", BigQueryTypeConverter.toDdlType(new DecimalType(50, 10)));
        assertEquals("ARRAY<STRING>", BigQueryTypeConverter.toDdlType(ArrayType.STRING_ARRAY_TYPE));
        assertEquals(
                "STRUCT<`id` INT64, `name` STRING>",
                BigQueryTypeConverter.toDdlType(
                        new SeaTunnelRowType(
                                new String[] {"id", "name"},
                                new org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] {
                                    BasicType.LONG_TYPE, BasicType.STRING_TYPE
                                })));
    }

    @Test
    void testRejectUnsupportedMapType() {
        assertThrows(
                BigQueryConnectorException.class,
                () ->
                        BigQueryTypeConverter.toDdlType(
                                new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE)));
    }
}
