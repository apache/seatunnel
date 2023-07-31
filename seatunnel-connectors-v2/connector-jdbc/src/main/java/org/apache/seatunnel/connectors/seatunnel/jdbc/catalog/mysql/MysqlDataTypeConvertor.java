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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql;

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

import org.apache.commons.collections4.MapUtils;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.mysql.cj.MysqlType;

import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@AutoService(DataTypeConvertor.class)
public class MysqlDataTypeConvertor implements DataTypeConvertor<MysqlType> {

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";

    public static final Integer DEFAULT_PRECISION = 10;

    public static final Integer DEFAULT_SCALE = 0;

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        checkNotNull(connectorDataType, "connectorDataType can not be null");
        MysqlType mysqlType = MysqlType.getByName(connectorDataType);
        Map<String, Object> dataTypeProperties;
        switch (mysqlType) {
            case BIGINT_UNSIGNED:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case BIT:
                // parse precision and scale
                int left = connectorDataType.indexOf("(");
                int right = connectorDataType.indexOf(")");
                int precision = DEFAULT_PRECISION;
                int scale = DEFAULT_SCALE;
                if (left != -1 && right != -1) {
                    String[] precisionAndScale =
                            connectorDataType.substring(left + 1, right).split(",");
                    if (precisionAndScale.length == 2) {
                        precision = Integer.parseInt(precisionAndScale[0]);
                        scale = Integer.parseInt(precisionAndScale[1]);
                    } else if (precisionAndScale.length == 1) {
                        precision = Integer.parseInt(precisionAndScale[0]);
                    }
                }
                dataTypeProperties = ImmutableMap.of(PRECISION, precision, SCALE, scale);
                break;
            default:
                dataTypeProperties = Collections.emptyMap();
                break;
        }
        return toSeaTunnelType(mysqlType, dataTypeProperties);
    }

    // todo: It's better to wrapper MysqlType to a pojo in ST, since MysqlType doesn't contains
    // properties.
    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            MysqlType mysqlType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        checkNotNull(mysqlType, "mysqlType can not be null");
        int precision;
        int scale;
        switch (mysqlType) {
            case NULL:
                return BasicType.VOID_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case BIT:
                precision = (Integer) dataTypeProperties.get(MysqlDataTypeConvertor.PRECISION);
                if (precision == 1) {
                    return BasicType.BOOLEAN_TYPE;
                } else {
                    return PrimitiveByteArrayType.INSTANCE;
                }
            case TINYINT:
                return BasicType.BYTE_TYPE;
            case TINYINT_UNSIGNED:
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case SMALLINT_UNSIGNED:
            case INT:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case YEAR:
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
                precision = MapUtils.getInteger(dataTypeProperties, PRECISION, DEFAULT_PRECISION);
                scale = MapUtils.getInteger(dataTypeProperties, SCALE, DEFAULT_SCALE);
                return new DecimalType(precision, scale);
                // TODO: support 'SET' & 'YEAR' type
            default:
                throw DataTypeConvertException.convertToSeaTunnelDataTypeException(mysqlType);
        }
    }

    @Override
    public MysqlType toConnectorType(
            SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        SqlType sqlType = seaTunnelDataType.getSqlType();
        // todo: verify
        switch (sqlType) {
                // from pg array not support
                //            case ARRAY:
                //                return MysqlType.ENUM;
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
                return MysqlType.TIME;
            case TIMESTAMP:
                return MysqlType.DATETIME;
            default:
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format("Doesn't support MySQL type '%s' yet", sqlType));
        }
    }

    @Override
    public String getIdentity() {
        return "Mysql";
    }
}
