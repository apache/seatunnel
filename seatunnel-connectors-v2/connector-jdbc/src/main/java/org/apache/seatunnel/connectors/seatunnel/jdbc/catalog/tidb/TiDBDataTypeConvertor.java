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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.tidb;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableMap;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MysqlDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeConverter;

import com.google.auto.service.AutoService;
import com.mysql.cj.MysqlType;

import java.util.Collections;
import java.util.Map;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

/** @deprecated instead by {@link MySqlTypeConverter} */
@Deprecated
@AutoService(DataTypeConvertor.class)
public class TiDBDataTypeConvertor implements DataTypeConvertor<MysqlType> {

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";

    public static final Integer DEFAULT_PRECISION = 10;

    public static final Integer DEFAULT_SCALE = 0;
    private static final MysqlDataTypeConvertor MYSQL_CONVERTOR = new MysqlDataTypeConvertor();

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType) {
        checkNotNull(connectorDataType, "connectorDataType can not be null");
        MysqlType mysqlType = MysqlType.getByName(connectorDataType);
        Map<String, Object> dataTypeProperties;
        switch (mysqlType) {
            case BIGINT_UNSIGNED:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case BIT:
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
        return toSeaTunnelType(field, mysqlType, dataTypeProperties);
    }

    // todo: It's better to wrapper MysqlType to a pojo in ST, since MysqlType doesn't contains
    // properties.
    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, MysqlType mysqlType, Map<String, Object> dataTypeProperties) {
        try {
            return MYSQL_CONVERTOR.toSeaTunnelType(field, mysqlType, dataTypeProperties);
        } catch (SeaTunnelRuntimeException e) {
            if (CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE.equals(
                    e.getSeaTunnelErrorCode())) {
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.TIDB, mysqlType.getName(), field);
            }
            throw e;
        }
    }

    @Override
    public MysqlType toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) {
        try {
            return MYSQL_CONVERTOR.toConnectorType(field, seaTunnelDataType, dataTypeProperties);
        } catch (SeaTunnelRuntimeException e) {
            if (CommonErrorCode.CONVERT_TO_CONNECTOR_TYPE_ERROR_SIMPLE.equals(
                    e.getSeaTunnelErrorCode())) {
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.TIDB, seaTunnelDataType.getSqlType().name(), field);
            }
            throw e;
        }
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.TIDB;
    }
}
