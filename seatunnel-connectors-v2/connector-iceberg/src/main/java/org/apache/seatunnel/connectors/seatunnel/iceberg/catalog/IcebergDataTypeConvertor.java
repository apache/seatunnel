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

package org.apache.seatunnel.connectors.seatunnel.iceberg.catalog;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorException;

import org.apache.iceberg.types.Type;

import com.google.auto.service.AutoService;

import java.util.Locale;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.BasicType.BOOLEAN_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.DOUBLE_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.FLOAT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TIME_TYPE;
import static org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TYPE;
import static org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_TIME_TYPE;

@AutoService(DataTypeConvertor.class)
public class IcebergDataTypeConvertor implements DataTypeConvertor<String> {

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        if (connectorDataType == null) {
            return null;
        }
        Type.TypeID typeID = Type.TypeID.valueOf(connectorDataType.toUpperCase(Locale.ROOT));
        switch (typeID) {
            case BOOLEAN:
                return BOOLEAN_TYPE;
            case INTEGER:
                return INT_TYPE;
            case LONG:
                return LONG_TYPE;
            case FLOAT:
                return FLOAT_TYPE;
            case DOUBLE:
                return DOUBLE_TYPE;
            case DATE:
                return LOCAL_DATE_TYPE;
            case TIME:
                return LOCAL_TIME_TYPE;
            case TIMESTAMP:
                return LOCAL_DATE_TIME_TYPE;
            case STRING:
                return STRING_TYPE;
            case FIXED:
            case BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case DECIMAL:
                return new DecimalType(38, 18);
            case STRUCT:
            case LIST:
            case MAP:
            default:
                throw new IcebergConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format("Unsupported iceberg type: %s", typeID));
        }
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String connectorDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        return toSeaTunnelType(connectorDataType);
    }

    @Override
    public String toConnectorType(
            SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        SqlType sqlType = seaTunnelDataType.getSqlType();
        switch (sqlType) {
            case MAP:
            case ROW:
            case STRING:
            case NULL:
                return Type.TypeID.STRING.toString();
            case BOOLEAN:
                return Type.TypeID.BOOLEAN.toString();
            case TINYINT:
            case SMALLINT:
            case INT:
                return Type.TypeID.INTEGER.toString();
            case BIGINT:
                return Type.TypeID.LONG.toString();
            case FLOAT:
                return Type.TypeID.FLOAT.toString();
            case DOUBLE:
                return Type.TypeID.DOUBLE.toString();
            case DECIMAL:
                return Type.TypeID.DECIMAL.toString();
            case BYTES:
                return Type.TypeID.FIXED.toString();
            case DATE:
                return Type.TypeID.DATE.toString();
            case TIME:
                return Type.TypeID.TIME.toString();
            case TIMESTAMP:
                return Type.TypeID.TIMESTAMP.toString();
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support Iceberg type '%s''  yet.", sqlType));
        }
    }

    @Override
    public String getIdentity() {
        return "Iceberg";
    }
}
