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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

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

import com.mysql.cj.MysqlType;
import org.apache.commons.collections4.MapUtils;

import java.util.Map;

public class MysqlDataTypeConvertor implements DataTypeConvertor<MysqlType> {

    private MysqlDataTypeConvertor() {

    }

    private static final MysqlDataTypeConvertor INSTANCE = new MysqlDataTypeConvertor();

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";

    // todo: It's better to wrapper MysqlType to a pojo in ST, since MysqlType doesn't contains properties.
    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(MysqlType mysqlType, Map<String, Object> dataTypeProperties) throws DataTypeConvertException {
        switch (mysqlType) {
            case NULL:
                return BasicType.VOID_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case BIT:
            case TINYINT:
                return BasicType.BYTE_TYPE;
            case TINYINT_UNSIGNED:
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case SMALLINT_UNSIGNED:
            case INT:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
                return BasicType.INT_TYPE;
            case INT_UNSIGNED:
            case BIGINT:
                return BasicType.LONG_TYPE;
            case FLOAT:
            case FLOAT_UNSIGNED:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                return BasicType.DOUBLE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIMESTAMP:
            case DATETIME:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            // TODO: to confirm
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case ENUM:
                return BasicType.STRING_TYPE;
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
            case GEOMETRY:
                return PrimitiveByteArrayType.INSTANCE;
            case BIGINT_UNSIGNED:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                Integer precision = MapUtils.getInteger(dataTypeProperties, PRECISION);
                Integer scale = MapUtils.getInteger(dataTypeProperties, SCALE);
                if (precision == null || scale == null) {
                    throw DataTypeConvertException.convertToSeaTunnelDataTypeException(mysqlType,
                        new IllegalArgumentException("Decimal type must have precision and scale"));
                }
                return new DecimalType(precision, scale);
            // TODO: support 'SET' & 'YEAR' type
            default:
                throw DataTypeConvertException.convertToSeaTunnelDataTypeException(mysqlType);
        }
    }

    @Override
    public MysqlType toConnectorType(SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties) throws DataTypeConvertException {
        SqlType sqlType = seaTunnelDataType.getSqlType();
        // todo: verify
        switch (sqlType) {
            case ARRAY:
            case MAP:
            case ROW:
            case STRING:
                return MysqlType.VARCHAR;
            case BOOLEAN:
                return MysqlType.BOOLEAN;
            case TINYINT:
                return MysqlType.TINYINT;
            case SMALLINT:
                return MysqlType.SMALLINT;
            case INT:
                return MysqlType.INT;
            case BIGINT:
                return MysqlType.BIGINT;
            case FLOAT:
                return MysqlType.FLOAT;
            case DOUBLE:
                return MysqlType.DOUBLE;
            case DECIMAL:
                return MysqlType.DECIMAL;
            case NULL:
                return MysqlType.NULL;
            case BYTES:
                return MysqlType.BIT;
            case DATE:
                return MysqlType.DATE;
            case TIME:
                return MysqlType.DATETIME;
            case TIMESTAMP:
                return MysqlType.TIMESTAMP;
            default:
                throw new JdbcConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, String.format("Doesn't support MySQL type '%s' yet", sqlType));

        }
    }

    public static MysqlDataTypeConvertor getInstance() {
        return INSTANCE;
    }
}
