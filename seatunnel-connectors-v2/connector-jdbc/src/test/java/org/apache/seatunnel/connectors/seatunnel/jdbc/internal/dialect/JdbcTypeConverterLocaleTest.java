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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Locale;

/** Verifies that the dialect type converters do not depend on the JVM default locale. */
public class JdbcTypeConverterLocaleTest {

    @Test
    public void testConvertUnderTurkishLocale() {
        Locale defaultLocale = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));

            Assertions.assertEquals(
                    BasicType.INT_TYPE,
                    DB2TypeConverter.INSTANCE.convert(typeDefine("integer")).getDataType());
            Assertions.assertEquals(
                    BasicType.INT_TYPE,
                    DmdbTypeConverter.INSTANCE.convert(typeDefine("int")).getDataType());
            Assertions.assertEquals(
                    BasicType.INT_TYPE,
                    IrisTypeConverter.INSTANCE.convert(typeDefine("integer")).getDataType());
            Assertions.assertEquals(
                    BasicType.INT_TYPE,
                    KingbaseTypeConverter.INSTANCE.convert(typeDefine("int")).getDataType());
            Assertions.assertEquals(
                    new DecimalType(OracleTypeConverter.DEFAULT_PRECISION, 0),
                    OracleTypeConverter.INSTANCE.convert(typeDefine("integer")).getDataType());
            Assertions.assertEquals(
                    BasicType.INT_TYPE,
                    PostgresTypeConverter.INSTANCE.convert(typeDefine("INT4")).getDataType());
            Assertions.assertEquals(
                    BasicType.INT_TYPE,
                    RedshiftTypeConverter.INSTANCE.convert(typeDefine("integer")).getDataType());
            Assertions.assertEquals(
                    BasicType.INT_TYPE,
                    SapHanaTypeConverter.INSTANCE.convert(typeDefine("integer")).getDataType());
            Assertions.assertEquals(
                    BasicType.INT_TYPE,
                    XuguTypeConverter.INSTANCE.convert(typeDefine("integer")).getDataType());
            Assertions.assertEquals(
                    BasicType.INT_TYPE,
                    YashanDbTypeConverter.INSTANCE.convert(typeDefine("integer")).getDataType());
        } finally {
            Locale.setDefault(defaultLocale);
        }
    }

    private static BasicTypeDefine<Object> typeDefine(String dataType) {
        return BasicTypeDefine.builder()
                .name("test")
                .columnType(dataType)
                .dataType(dataType)
                .build();
    }
}
