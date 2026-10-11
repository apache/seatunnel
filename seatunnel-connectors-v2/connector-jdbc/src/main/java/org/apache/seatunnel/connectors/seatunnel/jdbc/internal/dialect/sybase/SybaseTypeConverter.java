/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sybase;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(TypeConverter.class)
public class SybaseTypeConverter implements TypeConverter<BasicTypeDefine> {

    // -------------------------number----------------------------
    public static final String SYBASE_BIT = "BIT";
    public static final String SYBASE_TINYINT = "TINYINT";
    public static final String SYBASE_SMALLINT = "SMALLINT";
    public static final String SYBASE_INT = "INT";
    public static final String SYBASE_INTEGER = "INTEGER";
    public static final String SYBASE_BIGINT = "BIGINT";
    public static final String SYBASE_DECIMAL = "DECIMAL";
    public static final String SYBASE_NUMERIC = "NUMERIC";
    public static final String SYBASE_FLOAT = "FLOAT";
    public static final String SYBASE_REAL = "REAL";
    public static final String SYBASE_MONEY = "MONEY";
    public static final String SYBASE_SMALLMONEY = "SMALLMONEY";

    // -------------------------string----------------------------
    public static final String SYBASE_CHAR = "CHAR";
    public static final String SYBASE_VARCHAR = "VARCHAR";
    public static final String SYBASE_NCHAR = "NCHAR";
    public static final String SYBASE_NVARCHAR = "NVARCHAR";
    public static final String SYBASE_TEXT = "TEXT";

    // ------------------------------time-------------------------
    public static final String SYBASE_DATE = "DATE";
    public static final String SYBASE_TIME = "TIME";
    public static final String SYBASE_DATETIME = "DATETIME";
    public static final String SYBASE_SMALLDATETIME = "SMALLDATETIME";

    public static final SybaseTypeConverter INSTANCE = new SybaseTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.SYBASE;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        String sybaseType = typeDefine.getDataType().toUpperCase().replace(" IDENTITY", "");

        switch (sybaseType) {
            case SYBASE_BIT:
                builder.sourceType(SYBASE_BIT);
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case SYBASE_TINYINT:
            case SYBASE_SMALLINT:
                builder.sourceType(sybaseType);
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case SYBASE_INT:
            case SYBASE_INTEGER:
                builder.sourceType(SYBASE_INT);
                builder.dataType(BasicType.INT_TYPE);
                break;
            case SYBASE_BIGINT:
                builder.sourceType(SYBASE_BIGINT);
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case SYBASE_REAL:
                builder.sourceType(SYBASE_REAL);
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case SYBASE_FLOAT:
                builder.sourceType(SYBASE_FLOAT);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case SYBASE_DECIMAL:
            case SYBASE_NUMERIC:
            case SYBASE_MONEY:
            case SYBASE_SMALLMONEY:
                builder.sourceType(sybaseType);
                int precision =
                        typeDefine.getPrecision() != null
                                ? typeDefine.getPrecision().intValue()
                                : 38;
                int scale = typeDefine.getScale() != null ? typeDefine.getScale() : 18;
                builder.dataType(new DecimalType(precision, scale));
                builder.columnLength((long) precision);
                builder.scale(scale);
                break;
            case SYBASE_CHAR:
            case SYBASE_VARCHAR:
            case SYBASE_NCHAR:
            case SYBASE_NVARCHAR:
            case SYBASE_TEXT:
                builder.sourceType(sybaseType);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(
                        typeDefine.getLength() != null ? typeDefine.getLength() : 8000L);
                break;
            case SYBASE_DATE:
                builder.sourceType(SYBASE_DATE);
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case SYBASE_TIME:
                builder.sourceType(SYBASE_TIME);
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                break;
            case SYBASE_DATETIME:
            case SYBASE_SMALLDATETIME:
                builder.sourceType(sybaseType);
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.SYBASE, sybaseType, typeDefine.getName());
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());

        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
                builder.columnType(SYBASE_BIT).dataType(SYBASE_BIT);
                break;
            case TINYINT:
            case SMALLINT:
                builder.columnType(SYBASE_SMALLINT).dataType(SYBASE_SMALLINT);
                break;
            case INT:
                builder.columnType(SYBASE_INT).dataType(SYBASE_INT);
                break;
            case BIGINT:
                builder.columnType(SYBASE_BIGINT).dataType(SYBASE_BIGINT);
                break;
            case FLOAT:
                builder.columnType(SYBASE_REAL).dataType(SYBASE_REAL);
                break;
            case DOUBLE:
                builder.columnType(SYBASE_FLOAT).dataType(SYBASE_FLOAT);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                long precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                builder.columnType(String.format("%s(%s,%s)", SYBASE_DECIMAL, precision, scale));
                builder.dataType(SYBASE_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case STRING:
                builder.columnType(SYBASE_VARCHAR).dataType(SYBASE_VARCHAR);
                break;
            case DATE:
                builder.columnType(SYBASE_DATE).dataType(SYBASE_DATE);
                break;
            case TIME:
                builder.columnType(SYBASE_TIME).dataType(SYBASE_TIME);
                break;
            case TIMESTAMP:
                builder.columnType(SYBASE_DATETIME).dataType(SYBASE_DATETIME);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.SYBASE,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }
}
