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
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.transform.exception.TransformException;

import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.Arrays;
import java.util.List;

public class CastFunction {

    public static final String DECIMAL = "DECIMAL";
    public static final String VARCHAR = "VARCHAR";
    public static final String STRING = "STRING";
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
    public static final String INTEGER = "INTEGER";
    public static final String BIGINT = "BIGINT";
    public static final String LONG = "LONG";
    public static final String BYTE = "BYTE";
    public static final String BYTES = "BYTES";
    public static final String BINARY = "BINARY";
    public static final String DOUBLE = "DOUBLE";
    public static final String FLOAT = "FLOAT";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String DATETIME = "DATETIME";
    public static final String DATE = "DATE";
    public static final String TIME = "TIME";
    public static final String BOOLEAN = "BOOLEAN";

    public static final List<SqlType> INT_CAST_TYPE =
            Arrays.asList(
                    SqlType.TINYINT, SqlType.SMALLINT, SqlType.INT, SqlType.BIGINT, SqlType.STRING);
    public static final List<SqlType> LONG_CAST_TYPES =
            Arrays.asList(
                    SqlType.TINYINT, SqlType.SMALLINT, SqlType.INT, SqlType.BIGINT, SqlType.STRING);
    public static final List<SqlType> FLOAT_CAST_TYPES =
            Arrays.asList(
                    SqlType.TINYINT,
                    SqlType.SMALLINT,
                    SqlType.INT,
                    SqlType.BIGINT,
                    SqlType.FLOAT,
                    SqlType.DOUBLE,
                    SqlType.STRING);
    public static final List<SqlType> BOOLEAN_CAST_TYPES =
            Arrays.asList(
                    SqlType.BOOLEAN,
                    SqlType.STRING,
                    SqlType.BIGINT,
                    SqlType.INT,
                    SqlType.SMALLINT,
                    SqlType.TINYINT,
                    SqlType.FLOAT,
                    SqlType.DOUBLE);
    public static final List<SqlType> DATETIME_CAST_TYPES =
            Arrays.asList(SqlType.TIMESTAMP, SqlType.TIMESTAMP_TZ, SqlType.BIGINT);
    public static final List<SqlType> DATE_CAST_TYPES =
            Arrays.asList(SqlType.TIMESTAMP, SqlType.TIMESTAMP_TZ, SqlType.DATE, SqlType.INT);
    public static final List<SqlType> TIME_CAST_TYPES =
            Arrays.asList(SqlType.TIMESTAMP, SqlType.TIMESTAMP_TZ, SqlType.TIME, SqlType.INT);

    public static SeaTunnelDataType<?> getCastType(SqlType originType, ColDataType colDataType) {
        String dataType = colDataType.getDataType();
        switch (dataType.toUpperCase()) {
            case DECIMAL:
                List<String> ps = colDataType.getArgumentsStringList();
                return new DecimalType(Integer.parseInt(ps.get(0)), Integer.parseInt(ps.get(1)));
            case VARCHAR:
            case STRING:
                return BasicType.STRING_TYPE;
            case BYTE:
            case TINYINT:
                if (SqlType.TINYINT.equals(originType) || SqlType.STRING.equals(originType)) {
                    return BasicType.BYTE_TYPE;
                }
                break;
            case SMALLINT:
                if (SqlType.TINYINT.equals(originType)
                        || SqlType.SMALLINT.equals(originType)
                        || SqlType.STRING.equals(originType)) {
                    return BasicType.SHORT_TYPE;
                }
                break;
            case INT:
            case INTEGER:
                if (INT_CAST_TYPE.contains(originType)) {
                    return BasicType.INT_TYPE;
                }
                break;
            case BIGINT:
            case LONG:
                if (LONG_CAST_TYPES.contains(originType)) {
                    return BasicType.LONG_TYPE;
                }
                break;
            case FLOAT:
                if (FLOAT_CAST_TYPES.contains(originType)) {
                    return BasicType.FLOAT_TYPE;
                }
                break;
            case DOUBLE:
                if (FLOAT_CAST_TYPES.contains(originType)) {
                    return BasicType.DOUBLE_TYPE;
                }
                break;
            case BYTES:
            case BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case TIMESTAMP:
            case DATETIME:
                if (DATETIME_CAST_TYPES.contains(originType)) {
                    return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                }
                break;
            case DATE:
                if (DATE_CAST_TYPES.contains(originType)) {
                    return LocalTimeType.LOCAL_DATE_TYPE;
                }
                break;
            case TIME:
                if (TIME_CAST_TYPES.contains(originType)) {
                    return LocalTimeType.LOCAL_TIME_TYPE;
                }
                break;
            case BOOLEAN:
                if (BOOLEAN_CAST_TYPES.contains(originType)) {
                    return BasicType.BOOLEAN_TYPE;
                }
                break;
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported CAST FROM %s AS type: %s", originType.name(), dataType));
    }
}
