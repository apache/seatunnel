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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

// reference https://help.kingbase.com.cn/v8/development/sql-plsql/sql/datatype.html#id2
@Slf4j
@AutoService(TypeConverter.class)
public class KingbaseTypeConverter extends PostgresTypeConverter {
    public static final String KB_TINYINT = "TINYINT";
    public static final String KB_MONEY = "MONEY";
    public static final String KB_BLOB = "BLOB";
    public static final String KB_CLOB = "CLOB";
    public static final String KB_BIT = "BIT";

    public static final KingbaseTypeConverter INSTANCE = new KingbaseTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.KINGBASE;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        try {
            return super.convert(typeDefine);
        } catch (SeaTunnelRuntimeException e) {
            PhysicalColumn.PhysicalColumnBuilder builder =
                    PhysicalColumn.builder()
                            .name(typeDefine.getName())
                            .sourceType(typeDefine.getColumnType())
                            .nullable(typeDefine.isNullable())
                            .defaultValue(typeDefine.getDefaultValue())
                            .comment(typeDefine.getComment());

            String kingbaseDataType = typeDefine.getDataType().toUpperCase();
            switch (kingbaseDataType) {
                    // MySQL compatibility - only types not in PostgresTypeConverter
                    // int not in PG (PG has SMALLINT/INTEGER/BIGINT)
                case MySqlTypeConverter.MYSQL_SMALLINT_UNSIGNED:
                case MySqlTypeConverter.MYSQL_MEDIUMINT:
                case MySqlTypeConverter.MYSQL_MEDIUMINT_UNSIGNED:
                case MySqlTypeConverter.MYSQL_INT:
                case MySqlTypeConverter.MYSQL_INTEGER:
                case MySqlTypeConverter.MYSQL_YEAR:
                case MySqlTypeConverter.MYSQL_YEAR_UNSIGNED:
                    builder.dataType(BasicType.INT_TYPE);
                    break;
                    // DATETIME not in PG (PG has TIMESTAMP) — NTZ
                case MySqlTypeConverter.MYSQL_DATETIME:
                    builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                    if (typeDefine.getScale() != null
                            && typeDefine.getScale() > MAX_TIMESTAMP_SCALE) {
                        builder.scale(MAX_TIMESTAMP_SCALE);
                        log.warn(
                                "The timestamp column {} type timestamp({}) is out of range, "
                                        + "which exceeds the maximum scale of {}, "
                                        + "it will be converted to timestamp({})",
                                typeDefine.getName(),
                                typeDefine.getScale(),
                                MAX_TIMESTAMP_SCALE,
                                MAX_TIMESTAMP_SCALE);
                    } else {
                        builder.scale(typeDefine.getScale());
                    }
                    break;
                    // Binary types not in PG (PG has BYTEA)
                case MySqlTypeConverter.MYSQL_BINARY:
                case MySqlTypeConverter.MYSQL_VARBINARY:
                case MySqlTypeConverter.MYSQL_TINYBLOB:
                case MySqlTypeConverter.MYSQL_MEDIUMBLOB:
                case MySqlTypeConverter.MYSQL_LONGBLOB:
                    builder.dataType(PrimitiveByteArrayType.INSTANCE);
                    if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                        builder.columnLength(typeDefine.getLength());
                    } else {
                        builder.columnLength((long) (1024 * 1024 * 1024));
                    }
                    break;
                    // Text types not in PG (PG has TEXT/VARCHAR/CHAR)
                case MySqlTypeConverter.MYSQL_TINYTEXT:
                case MySqlTypeConverter.MYSQL_MEDIUMTEXT:
                case MySqlTypeConverter.MYSQL_LONGTEXT:
                    builder.dataType(BasicType.STRING_TYPE);
                    if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                        builder.columnLength(typeDefine.getLength());
                    }
                    break;
                    // Oracle compatibility - Oracle specific types (not in PostgresTypeConverter)
                    // NUMBER is Oracle-specific numeric type
                case OracleTypeConverter.ORACLE_NUMBER:
                    DecimalType oracleDecimal =
                            new DecimalType(
                                    typeDefine.getPrecision() == null
                                            ? DEFAULT_PRECISION
                                            : typeDefine.getPrecision().intValue(),
                                    typeDefine.getScale() == null ? 0 : typeDefine.getScale());
                    builder.dataType(oracleDecimal);
                    builder.columnLength((long) oracleDecimal.getPrecision());
                    builder.scale(oracleDecimal.getScale());
                    break;
                    // FLOAT is different from PG FLOAT
                case OracleTypeConverter.ORACLE_FLOAT:
                    DecimalType floatDecimal = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                    builder.dataType(floatDecimal);
                    builder.columnLength((long) floatDecimal.getPrecision());
                    builder.scale(floatDecimal.getScale());
                    break;
                    // Oracle string types (VARCHAR2, NVARCHAR2, NCHAR differ from PG)
                case OracleTypeConverter.ORACLE_VARCHAR2:
                case OracleTypeConverter.ORACLE_NVARCHAR2:
                case OracleTypeConverter.ORACLE_NCHAR:
                case OracleTypeConverter.ORACLE_LONG:
                case OracleTypeConverter.ORACLE_ROWID:
                case OracleTypeConverter.ORACLE_NCLOB:
                case OracleTypeConverter.ORACLE_XML:
                case OracleTypeConverter.ORACLE_SYS_XML:
                    builder.dataType(BasicType.STRING_TYPE);
                    if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                        builder.columnLength(typeDefine.getLength());
                    } else {
                        builder.columnLength((long) (1024 * 1024 * 1024));
                    }
                    break;
                    // MySQL TIMESTAMP — LTZ (timezone-aware)
                case MySqlTypeConverter.MYSQL_TIMESTAMP:
                    builder.dataType(LocalTimeType.OFFSET_DATE_TIME_TYPE);
                    builder.scale(typeDefine.getScale());
                    break;
                    // SQLServer compatibility - NTZ types
                case SqlServerTypeConverter.SQLSERVER_DATETIME2:
                case SqlServerTypeConverter.SQLSERVER_SMALLDATETIME:
                    builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                    if (typeDefine.getScale() != null
                            && typeDefine.getScale() > MAX_TIMESTAMP_SCALE) {
                        builder.scale(MAX_TIMESTAMP_SCALE);
                        log.warn(
                                "The timestamp column {} type timestamp({}) is out of range, "
                                        + "which exceeds the maximum scale of {}, "
                                        + "it will be converted to timestamp({})",
                                typeDefine.getName(),
                                typeDefine.getScale(),
                                MAX_TIMESTAMP_SCALE,
                                MAX_TIMESTAMP_SCALE);
                    } else {
                        builder.scale(typeDefine.getScale());
                    }
                    break;
                    // SQLServer DATETIMEOFFSET — LTZ (timezone-aware)
                case SqlServerTypeConverter.SQLSERVER_DATETIMEOFFSET:
                    builder.dataType(LocalTimeType.OFFSET_DATE_TIME_TYPE);
                    builder.scale(typeDefine.getScale());
                    break;
                case KB_TINYINT:
                    builder.dataType(BasicType.BYTE_TYPE);
                    break;
                case KB_MONEY:
                    builder.dataType(new DecimalType(38, 18));
                    builder.columnLength(38L);
                    builder.scale(18);
                    break;
                case KB_BLOB:
                    builder.dataType(PrimitiveByteArrayType.INSTANCE);
                    builder.columnLength((long) (1024 * 1024 * 1024));
                    break;
                case KB_CLOB:
                    builder.dataType(BasicType.STRING_TYPE);
                    builder.columnLength(typeDefine.getLength());
                    builder.columnLength((long) (1024 * 1024 * 1024));
                    break;
                case KB_BIT:
                    builder.dataType(PrimitiveByteArrayType.INSTANCE);
                    // BIT(M) -> BYTE(M/8)
                    long byteLength = typeDefine.getLength() / 8;
                    byteLength += typeDefine.getLength() % 8 > 0 ? 1 : 0;
                    builder.columnLength(byteLength);
                    break;
                default:
                    throw CommonError.convertToSeaTunnelTypeError(
                            DatabaseIdentifier.KINGBASE,
                            typeDefine.getDataType(),
                            typeDefine.getName());
            }
            return builder.build();
        }
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        try {
            return super.reconvert(column);
        } catch (SeaTunnelRuntimeException e) {
            throw CommonError.convertToConnectorTypeError(
                    DatabaseIdentifier.KINGBASE,
                    column.getDataType().getSqlType().name(),
                    column.getName());
        }
    }
}
