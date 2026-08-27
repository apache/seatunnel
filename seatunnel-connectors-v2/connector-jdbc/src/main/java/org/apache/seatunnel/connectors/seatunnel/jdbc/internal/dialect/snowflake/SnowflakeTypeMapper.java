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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

@Slf4j
public class SnowflakeTypeMapper implements JdbcDialectTypeMapper {

    private static final String SNOWFLAKE_VARCHAR = "VARCHAR";
    private static final String SNOWFLAKE_CHAR = "CHAR";
    private static final String SNOWFLAKE_CHARACTER = "CHARACTER";
    private static final String SNOWFLAKE_STRING = "STRING";
    private static final String SNOWFLAKE_TEXT = "TEXT";
    private static final String SNOWFLAKE_VARIANT = "VARIANT";
    private static final String SNOWFLAKE_OBJECT = "OBJECT";

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return SnowflakeTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnName = metadata.getColumnLabel(colIndex);
        String nativeType = metadata.getColumnTypeName(colIndex);
        int isNullable = metadata.isNullable(colIndex);
        long precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        if (Arrays.asList(
                        SNOWFLAKE_CHAR,
                        SNOWFLAKE_OBJECT,
                        SNOWFLAKE_TEXT,
                        SNOWFLAKE_VARCHAR,
                        SNOWFLAKE_CHARACTER,
                        SNOWFLAKE_STRING,
                        SNOWFLAKE_VARIANT)
                .contains(nativeType)) {
            long octetLength = TypeDefineUtils.charTo4ByteLength(precision);
            precision = Math.max(precision, octetLength);
        }

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(nativeType)
                        .dataType(nativeType)
                        .nullable(isNullable == ResultSetMetaData.columnNullable)
                        .length(precision)
                        .precision(precision)
                        .scale(scale)
                        .build();
        return mappingColumn(typeDefine);
    }
}
