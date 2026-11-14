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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect;

import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.db2.DB2TypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.iris.IrisTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase.KingbaseTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.redshift.RedshiftTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.saphana.SapHanaTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.xugu.XuguTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb.YashanDbTypeConverter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Locale;
import java.util.stream.Stream;

/**
 * Verifies that the dialect type converters do not depend on the JVM default locale.
 *
 * <p>Every converter runs as its own parameterized invocation, so a regression in a single dialect
 * fails with that converter's name instead of hiding inside one combined assertion.
 *
 * <p>The default locale is switched to Turkish, where {@code i} upper-cases to a dotted capital
 * {@code I} ({@code U+0130}), in {@link BeforeEach} and restored in {@link AfterEach}: the previous
 * base, {@code DISPLAY} and {@code FORMAT} defaults are captured before the switch, and JUnit runs
 * the restore even when the converter call throws, so a Turkish default cannot leak into other
 * tests sharing the JVM. {@link ResourceLock} keeps the class out of the way if parallel execution
 * is enabled later.
 */
@ResourceLock("java.util.Locale")
public class JdbcTypeConverterLocaleTest {

    private Locale base;
    private Locale display;
    private Locale format;

    @BeforeEach
    public void switchToTurkishLocale() {
        base = Locale.getDefault();
        display = Locale.getDefault(Locale.Category.DISPLAY);
        format = Locale.getDefault(Locale.Category.FORMAT);
        Locale.setDefault(new Locale("tr", "TR"));
    }

    @AfterEach
    public void restoreDefaultLocale() {
        Locale.setDefault(base);
        Locale.setDefault(Locale.Category.DISPLAY, display);
        Locale.setDefault(Locale.Category.FORMAT, format);
    }

    @ParameterizedTest(name = "{0} maps {2}")
    @MethodSource("converters")
    public void convertsSourceTypeUnderTurkishLocale(
            String dialect,
            TypeConverter<BasicTypeDefine> converter,
            String sourceType,
            SeaTunnelDataType<?> expectedType) {
        Assertions.assertEquals(
                expectedType,
                converter.convert(typeDefine(sourceType)).getDataType(),
                dialect + " folded " + sourceType + " to the wrong type under tr-TR");
    }

    private static Stream<Arguments> converters() {
        return Stream.of(
                Arguments.of(
                        "DB2TypeConverter",
                        DB2TypeConverter.INSTANCE,
                        "integer",
                        BasicType.INT_TYPE),
                Arguments.of(
                        "DmdbTypeConverter", DmdbTypeConverter.INSTANCE, "int", BasicType.INT_TYPE),
                Arguments.of(
                        "IrisTypeConverter",
                        IrisTypeConverter.INSTANCE,
                        "integer",
                        BasicType.INT_TYPE),
                Arguments.of(
                        "KingbaseTypeConverter",
                        KingbaseTypeConverter.INSTANCE,
                        "int",
                        BasicType.INT_TYPE),
                Arguments.of(
                        "OracleTypeConverter",
                        OracleTypeConverter.INSTANCE,
                        "integer",
                        new DecimalType(OracleTypeConverter.DEFAULT_PRECISION, 0)),
                Arguments.of(
                        "PostgresTypeConverter",
                        PostgresTypeConverter.INSTANCE,
                        "INT4",
                        BasicType.INT_TYPE),
                Arguments.of(
                        "RedshiftTypeConverter",
                        RedshiftTypeConverter.INSTANCE,
                        "integer",
                        BasicType.INT_TYPE),
                Arguments.of(
                        "SapHanaTypeConverter",
                        SapHanaTypeConverter.INSTANCE,
                        "integer",
                        BasicType.INT_TYPE),
                Arguments.of(
                        "XuguTypeConverter",
                        XuguTypeConverter.INSTANCE,
                        "integer",
                        BasicType.INT_TYPE),
                Arguments.of(
                        "YashanDbTypeConverter",
                        YashanDbTypeConverter.INSTANCE,
                        "integer",
                        BasicType.INT_TYPE));
    }

    private static BasicTypeDefine<Object> typeDefine(String dataType) {
        return BasicTypeDefine.builder()
                .name("test")
                .columnType(dataType)
                .dataType(dataType)
                .build();
    }
}
