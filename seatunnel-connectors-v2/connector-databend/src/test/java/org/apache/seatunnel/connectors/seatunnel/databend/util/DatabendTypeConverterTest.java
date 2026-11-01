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

package org.apache.seatunnel.connectors.seatunnel.databend.util;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Covers the SeaTunnel to Databend type mapping used to build the ALTER TABLE statements that carry
 * a schema change event into Databend.
 */
public class DatabendTypeConverterTest {

    @Test
    public void timeIsCarriedAsTextBecauseDatabendHasNoTimeType() {
        // Databend's type parser accepts TIME only inside TIMESTAMP WITH TIME ZONE, so naming it
        // as a column type produces a statement Databend cannot parse. A MySQL CDC source turns a
        // MySQL TIME column into LOCAL_TIME_TYPE, so this type does reach the converter.
        Assertions.assertEquals("STRING", columnTypeOf(LocalTimeType.LOCAL_TIME_TYPE));
        Assertions.assertNotEquals(
                "TIME",
                columnTypeOf(LocalTimeType.LOCAL_TIME_TYPE),
                "TIME is not a type Databend can parse in a column definition");
    }

    @Test
    public void theOtherTemporalTypesAreUnchanged() {
        Assertions.assertEquals("DATE", columnTypeOf(LocalTimeType.LOCAL_DATE_TYPE));
        Assertions.assertEquals("TIMESTAMP", columnTypeOf(LocalTimeType.LOCAL_DATE_TIME_TYPE));
    }

    @Test
    public void theRestOfTheMappingIsUnchanged() {
        Assertions.assertEquals("STRING", columnTypeOf(BasicType.STRING_TYPE));
        Assertions.assertEquals("BOOLEAN", columnTypeOf(BasicType.BOOLEAN_TYPE));
        Assertions.assertEquals("INT", columnTypeOf(BasicType.INT_TYPE));
        Assertions.assertEquals("BIGINT", columnTypeOf(BasicType.LONG_TYPE));
        Assertions.assertEquals("DECIMAL(10,2)", columnTypeOf(new DecimalType(10, 2)));
    }

    @Test
    public void theColumnNameAndNullabilityAreCarriedThrough() {
        Column column =
                PhysicalColumn.builder()
                        .name("started_at")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .nullable(false)
                        .build();

        BasicTypeDefine typeDefine = DatabendTypeConverter.convertToDatabendType(column);

        Assertions.assertEquals("started_at", typeDefine.getName());
        Assertions.assertEquals("STRING", typeDefine.getColumnType());
        Assertions.assertFalse(typeDefine.isNullable());
    }

    private static String columnTypeOf(SeaTunnelDataType<?> dataType) {
        Column column =
                PhysicalColumn.builder().name("c").dataType(dataType).nullable(true).build();
        return DatabendTypeConverter.convertToDatabendType(column).getColumnType();
    }
}
