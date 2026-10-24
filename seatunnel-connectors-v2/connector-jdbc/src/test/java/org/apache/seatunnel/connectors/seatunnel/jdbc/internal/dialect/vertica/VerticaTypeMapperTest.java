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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.vertica;

import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Covers the exact-numeric branch of {@link VerticaTypeMapper}: the Vertica JDBC driver reports
 * DECIMAL/NUMBER/MONEY columns as {@code NUMERIC}, which used to fall into the unsupported-type
 * error, and precisions above 38 must fall back to {@code DECIMAL(38,18)}.
 */
public class VerticaTypeMapperTest {

    private static final VerticaTypeMapper MAPPER = new VerticaTypeMapper();

    /** NUMERIC as reported by the driver maps like DECIMAL with the declared precision/scale. */
    @Test
    public void testNumericMapsToDecimalWithDeclaredPrecisionAndScale() throws SQLException {
        SeaTunnelDataType<?> type = MAPPER.mapping(metadata("NUMERIC", 10, 2), 1);

        Assertions.assertEquals(new DecimalType(10, 2), type);
    }

    /** Lower-case type names from the driver are normalized before matching. */
    @Test
    public void testLowerCaseNumericIsAccepted() throws SQLException {
        SeaTunnelDataType<?> type = MAPPER.mapping(metadata("numeric", 18, 4), 1);

        Assertions.assertEquals(new DecimalType(18, 4), type);
    }

    /** DECIMAL keeps working exactly as before the NUMERIC alias was added. */
    @Test
    public void testDecimalMapsToDecimalWithDeclaredPrecisionAndScale() throws SQLException {
        SeaTunnelDataType<?> type = MAPPER.mapping(metadata("DECIMAL", 12, 3), 1);

        Assertions.assertEquals(new DecimalType(12, 3), type);
    }

    /** Precisions above SeaTunnel's 38-digit maximum fall back to DECIMAL(38,18). */
    @Test
    public void testNumericAbovePrecisionLimitFallsBackToDecimal38_18() throws SQLException {
        Assertions.assertEquals(
                new DecimalType(38, 18), MAPPER.mapping(metadata("NUMERIC", 40, 5), 1));
        Assertions.assertEquals(
                new DecimalType(38, 18), MAPPER.mapping(metadata("DECIMAL", 1024, 15), 1));
    }

    /** Guards the regression that NUMERIC fixes: an unknown type still fails loudly. */
    @Test
    public void testUnsupportedTypeStillThrows() {
        ResultSetMetaData metadata = metadata("GEOMETRY", 0, 0);

        Assertions.assertThrows(SeaTunnelRuntimeException.class, () -> MAPPER.mapping(metadata, 1));
    }

    private static ResultSetMetaData metadata(String typeName, int precision, int scale) {
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        try {
            Mockito.when(metadata.getColumnTypeName(1)).thenReturn(typeName);
            Mockito.when(metadata.getPrecision(1)).thenReturn(precision);
            Mockito.when(metadata.getScale(1)).thenReturn(scale);
            Mockito.when(metadata.getColumnName(1)).thenReturn("col");
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return metadata;
    }
}
