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

package org.apache.seatunnel.connectors.seatunnel.databend.catalog;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Locale;

class DatabendCatalogTest {

    @Test
    void shouldMapParameterizedDecimalMetadata() {
        SeaTunnelDataType<?> dataType =
                DatabendCatalog.convertDatabendType("decimal(10, 2)", Types.DECIMAL, 10, 2);

        Assertions.assertTrue(dataType instanceof DecimalType);
        Assertions.assertEquals(10, ((DecimalType) dataType).getPrecision());
        Assertions.assertEquals(2, ((DecimalType) dataType).getScale());
    }

    @Test
    void shouldMapBinaryMetadataToBytes() {
        Assertions.assertSame(
                PrimitiveByteArrayType.INSTANCE,
                DatabendCatalog.convertDatabendType("binary", Types.BINARY, 255, 0));
        Assertions.assertSame(
                PrimitiveByteArrayType.INSTANCE,
                DatabendCatalog.convertDatabendType("varbinary", Types.VARBINARY, 255, 0));
    }

    @Test
    void shouldMapTypesIndependentOfDefaultLocale() {
        Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            Assertions.assertSame(
                    BasicType.BYTE_TYPE,
                    DatabendCatalog.convertDatabendType("tinyint", Types.TINYINT, 3, 0));
        } finally {
            Locale.setDefault(originalLocale);
        }
    }
}
