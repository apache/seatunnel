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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.commons.lang3.tuple.Pair;

import com.google.auto.service.AutoService;
import lombok.NonNull;

import java.util.Map;

@AutoService(DataTypeConvertor.class)
public class SqlServerDataTypeConvertor implements DataTypeConvertor<SqlServerType> {
    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";
    public static final Integer DEFAULT_PRECISION = 10;
    public static final Integer DEFAULT_SCALE = 0;

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(@NonNull String connectorDataType) {
        Pair<SqlServerType, Map<String, Object>> sqlServerType =
                SqlServerType.parse(connectorDataType);
        return toSeaTunnelType(sqlServerType.getLeft(), sqlServerType.getRight());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            @NonNull SqlServerType connectorDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        switch (connectorDataType) {
            case BIT:
                return BasicType.BOOLEAN_TYPE;
            case TINYINT:
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case INTEGER:
                return BasicType.INT_TYPE;
            case BIGINT:
                return BasicType.LONG_TYPE;
            case DECIMAL:
            case NUMERIC:
            case MONEY:
            case SMALLMONEY:
                int precision = (int) dataTypeProperties.getOrDefault(PRECISION, DEFAULT_PRECISION);
                int scale = (int) dataTypeProperties.getOrDefault(SCALE, DEFAULT_SCALE);
                return new DecimalType(precision, scale);
            case REAL:
                return BasicType.FLOAT_TYPE;
            case FLOAT:
                return BasicType.DOUBLE_TYPE;
            case CHAR:
            case NCHAR:
            case VARCHAR:
            case NTEXT:
            case NVARCHAR:
            case TEXT:
            case XML:
                return BasicType.STRING_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case DATETIME:
            case DATETIME2:
            case SMALLDATETIME:
            case DATETIMEOFFSET:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case TIMESTAMP:
            case BINARY:
            case VARBINARY:
            case IMAGE:
                return PrimitiveByteArrayType.INSTANCE;
            case UNKNOWN:
            default:
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format("Doesn't support SQLSERVER type '%s'", connectorDataType));
        }
    }

    @Override
    public SqlServerType toConnectorType(
            SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        SqlType sqlType = seaTunnelDataType.getSqlType();
        switch (sqlType) {
            case STRING:
                return SqlServerType.VARCHAR;
            case BOOLEAN:
                return SqlServerType.BIT;
            case TINYINT:
                return SqlServerType.TINYINT;
            case SMALLINT:
                return SqlServerType.SMALLINT;
            case INT:
                return SqlServerType.INTEGER;
            case BIGINT:
                return SqlServerType.BIGINT;
            case FLOAT:
                return SqlServerType.REAL;
            case DOUBLE:
                return SqlServerType.FLOAT;
            case DECIMAL:
                return SqlServerType.DECIMAL;
            case BYTES:
                return SqlServerType.BINARY;
            case DATE:
                return SqlServerType.DATE;
            case TIME:
                return SqlServerType.TIME;
            case TIMESTAMP:
                return SqlServerType.DATETIME2;
            default:
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format("Doesn't support SqlServer type '%s' yet", sqlType));
        }
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.SQLSERVER;
    }
}
