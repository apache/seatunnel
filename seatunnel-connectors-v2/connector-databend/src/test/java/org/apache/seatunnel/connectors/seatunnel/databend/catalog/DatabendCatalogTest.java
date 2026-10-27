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
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Locale;

/**
 * Covers the Databend to SeaTunnel type mapping the catalog applies to every column of every table
 * the source reads. The type names used here are the ones a Databend server actually reports:
 * information_schema.columns exposes data_type lower cased, and the JDBC driver passes that string
 * through as TYPE_NAME, keeping the arguments of a parameterised type.
 */
public class DatabendCatalogTest {

    @Test
    public void decimalKeepsItsPrecisionAndScale() {
        // A Databend DECIMAL arrives with its arguments attached, so an exact match against the
        // bare name "DECIMAL" never fires and the column used to degrade to STRING.
        Assertions.assertEquals(
                new DecimalType(10, 2),
                DatabendCatalog.convertDatabendType("decimal(10, 2)", Types.DECIMAL, 10, 2));
        Assertions.assertEquals(
                new DecimalType(38, 0),
                DatabendCatalog.convertDatabendType("decimal(38, 0)", Types.DECIMAL, 38, 0));
        // The unparameterised spelling has to keep working too.
        Assertions.assertEquals(
                new DecimalType(20, 4),
                DatabendCatalog.convertDatabendType("decimal", Types.DECIMAL, 20, 4));
    }

    @Test
    public void binaryMapsToBytesRatherThanASingleByte() {
        // BYTE_TYPE is one java.lang.Byte (SqlType.TINYINT). A Databend BINARY column holds a byte
        // string, and toDatabendTypeString writes SeaTunnel BYTES back out as VARBINARY, so the
        // reverse direction has to produce BYTES for the round trip to hold.
        Assertions.assertEquals(
                PrimitiveByteArrayType.INSTANCE,
                DatabendCatalog.convertDatabendType("binary", Types.BINARY, 0, 0));
        Assertions.assertEquals(
                PrimitiveByteArrayType.INSTANCE,
                DatabendCatalog.convertDatabendType("varbinary", Types.VARBINARY, 0, 0));
    }

    @Test
    public void theDocumentedMappingHolds() {
        assertMaps("boolean", BasicType.BOOLEAN_TYPE);
        assertMaps("tinyint", BasicType.BYTE_TYPE);
        assertMaps("smallint", BasicType.SHORT_TYPE);
        assertMaps("int", BasicType.INT_TYPE);
        assertMaps("bigint", BasicType.LONG_TYPE);
        assertMaps("float", BasicType.FLOAT_TYPE);
        assertMaps("double", BasicType.DOUBLE_TYPE);
        assertMaps("varchar", BasicType.STRING_TYPE);
        assertMaps("date", LocalTimeType.LOCAL_DATE_TYPE);
        assertMaps("timestamp", LocalTimeType.LOCAL_DATE_TIME_TYPE);
        assertMaps("binary", PrimitiveByteArrayType.INSTANCE);
    }

    @Test
    public void theMappingIsIndependentOfTheDefaultLocale() {
        // Under tr-TR and az-AZ an uppercase with the default locale turns the i of "int",
        // "bigint", "timestamp", "binary" and "decimal" into a dotted capital I, so none of those
        // names matches its branch any more and every one of those columns becomes a STRING.
        Locale previous = Locale.getDefault();
        try {
            for (Locale locale : new Locale[] {new Locale("tr", "TR"), new Locale("az", "AZ")}) {
                Locale.setDefault(locale);
                assertMaps("tinyint", BasicType.BYTE_TYPE);
                assertMaps("smallint", BasicType.SHORT_TYPE);
                assertMaps("int", BasicType.INT_TYPE);
                assertMaps("bigint", BasicType.LONG_TYPE);
                assertMaps("timestamp", LocalTimeType.LOCAL_DATE_TIME_TYPE);
                assertMaps("binary", PrimitiveByteArrayType.INSTANCE);
                Assertions.assertEquals(
                        new DecimalType(10, 2),
                        DatabendCatalog.convertDatabendType("decimal(10, 2)", Types.DECIMAL, 10, 2),
                        "a decimal column must not depend on the default locale");
            }
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    public void anUnknownTypeStillFallsBackToString() {
        // The fallback is deliberate: a type the connector has no column type for is carried as
        // text rather than failing the job.
        assertMaps("variant", BasicType.STRING_TYPE);
        assertMaps("geometry", BasicType.STRING_TYPE);
        assertMaps("array(int32)", BasicType.STRING_TYPE);
    }

    private static void assertMaps(String databendTypeName, SeaTunnelDataType<?> expected) {
        Assertions.assertEquals(
                expected,
                DatabendCatalog.convertDatabendType(databendTypeName, Types.OTHER, 0, 0),
                "unexpected SeaTunnel type for Databend type " + databendTypeName);
    }
}
