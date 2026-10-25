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

package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.Arrays;

public class CastFunctionTypeTest {

    private ColDataType col(String type, String... args) {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType(type);
        if (args != null && args.length > 0) {
            colDataType.setArgumentsStringList(Arrays.asList(args));
        }
        return colDataType;
    }

    private SeaTunnelDataType<?> castType(SqlType origin, String target, String... args) {
        return CastFunction.getCastType(origin, col(target, args));
    }

    @Test
    public void testDecimalCastType() {
        SeaTunnelDataType<?> type = castType(SqlType.INT, CastFunction.DECIMAL, "10", "2");
        Assertions.assertTrue(type instanceof DecimalType);
        DecimalType decimalType = (DecimalType) type;
        Assertions.assertEquals(10, decimalType.getPrecision());
        Assertions.assertEquals(2, decimalType.getScale());
    }

    @Test
    public void testIntegerFamilyCastTypes() {
        for (SqlType origin : CastFunction.INT_CAST_TYPE) {
            SeaTunnelDataType<?> type = castType(origin, CastFunction.INT);
            Assertions.assertEquals(BasicType.INT_TYPE, type);
        }

        for (SqlType origin : CastFunction.LONG_CAST_TYPES) {
            SeaTunnelDataType<?> type = castType(origin, CastFunction.BIGINT);
            Assertions.assertEquals(BasicType.LONG_TYPE, type);
        }

        // tinyint and smallint special rules
        Assertions.assertEquals(
                BasicType.BYTE_TYPE, castType(SqlType.TINYINT, CastFunction.TINYINT));
        Assertions.assertEquals(
                BasicType.BYTE_TYPE, castType(SqlType.STRING, CastFunction.TINYINT));

        Assertions.assertEquals(
                BasicType.SHORT_TYPE, castType(SqlType.TINYINT, CastFunction.SMALLINT));
        Assertions.assertEquals(
                BasicType.SHORT_TYPE, castType(SqlType.SMALLINT, CastFunction.SMALLINT));
        Assertions.assertEquals(
                BasicType.SHORT_TYPE, castType(SqlType.STRING, CastFunction.SMALLINT));
    }

    @Test
    public void testFloatAndDoubleCastTypes() {
        for (SqlType origin : CastFunction.FLOAT_CAST_TYPES) {
            SeaTunnelDataType<?> floatType = castType(origin, CastFunction.FLOAT);
            Assertions.assertEquals(BasicType.FLOAT_TYPE, floatType);

            SeaTunnelDataType<?> doubleType = castType(origin, CastFunction.DOUBLE);
            Assertions.assertEquals(BasicType.DOUBLE_TYPE, doubleType);
        }
    }

    @Test
    public void testBooleanCastTypes() {
        for (SqlType origin : CastFunction.BOOLEAN_CAST_TYPES) {
            SeaTunnelDataType<?> type = castType(origin, CastFunction.BOOLEAN);
            Assertions.assertEquals(BasicType.BOOLEAN_TYPE, type);
        }
    }

    @Test
    public void testStringAndBytesCastTypes() {
        // VARCHAR / STRING always map to STRING_TYPE
        Assertions.assertEquals(BasicType.STRING_TYPE, castType(SqlType.INT, CastFunction.VARCHAR));
        Assertions.assertEquals(
                BasicType.STRING_TYPE, castType(SqlType.BIGINT, CastFunction.STRING));

        // BYTES / BINARY always map to PrimitiveByteArrayType
        Assertions.assertEquals(
                PrimitiveByteArrayType.INSTANCE, castType(SqlType.STRING, CastFunction.BYTES));
        Assertions.assertEquals(
                PrimitiveByteArrayType.INSTANCE, castType(SqlType.INT, CastFunction.BINARY));
    }

    @Test
    public void testDateTimeFamilyCastTypes() {
        for (SqlType origin : CastFunction.DATETIME_CAST_TYPES) {
            SeaTunnelDataType<?> type = castType(origin, CastFunction.DATETIME);
            Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, type);
        }

        for (SqlType origin : CastFunction.DATE_CAST_TYPES) {
            SeaTunnelDataType<?> type = castType(origin, CastFunction.DATE);
            Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, type);
        }

        for (SqlType origin : CastFunction.TIME_CAST_TYPES) {
            SeaTunnelDataType<?> type = castType(origin, CastFunction.TIME);
            Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, type);
        }
    }

    @Test
    public void testUnsupportedCastCombinationsThrow() {
        // BOOLEAN cannot be cast to INT
        Assertions.assertThrows(
                TransformException.class, () -> castType(SqlType.BOOLEAN, CastFunction.INT));

        // DATE cannot be cast to TINYINT
        Assertions.assertThrows(
                TransformException.class, () -> castType(SqlType.DATE, CastFunction.TINYINT));

        // TIMESTAMP cannot be cast to BYTES via DECIMAL (nonsense target)
        Assertions.assertThrows(
                TransformException.class, () -> castType(SqlType.TIMESTAMP, "UNSUPPORTED_TYPE"));
    }
}
