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

package org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

public class KuduTypeMapper {

    private static final Logger log = LoggerFactory.getLogger(KuduTypeMapper.class);

    public static SeaTunnelDataType<?> mapping(List<ColumnSchema> columnSchemaList, int colIndex)
            throws SQLException {
        Type kuduType = columnSchemaList.get(colIndex).getType();
        switch (kuduType) {
            case BOOL:
                return BasicType.BOOLEAN_TYPE;
            case INT8:
                return BasicType.BYTE_TYPE;
            case INT16:
                return BasicType.SHORT_TYPE;
            case INT32:
                return BasicType.INT_TYPE;
            case INT64:
                return BasicType.LONG_TYPE;
            case DECIMAL:
                ColumnTypeAttributes typeAttributes =
                        columnSchemaList.get(colIndex).getTypeAttributes();
                return new DecimalType(typeAttributes.getPrecision(), typeAttributes.getScale());
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;

            case STRING:
                return BasicType.STRING_TYPE;
            case UNIXTIME_MICROS:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            default:
                throw new KuduConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format("Doesn't support KUDU type '%s' .", kuduType));
        }
    }
}
