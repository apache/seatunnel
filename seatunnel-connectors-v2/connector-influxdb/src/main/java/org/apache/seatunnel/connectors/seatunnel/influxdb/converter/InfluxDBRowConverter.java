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

package org.apache.seatunnel.connectors.seatunnel.influxdb.converter;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorException;

import java.util.ArrayList;
import java.util.List;

public class InfluxDBRowConverter {

    public static SeaTunnelRow convert(List<Object> values, SeaTunnelRowType typeInfo, List<Integer> indexList) {

        SeaTunnelDataType<?>[] seaTunnelDataTypes = typeInfo.getFieldTypes();
        List<Object> fields = new ArrayList<>(seaTunnelDataTypes.length);

        for (int i = 0; i <= seaTunnelDataTypes.length - 1; i++) {
            Object seaTunnelField;
            int columnIndex = indexList.get(i);
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[i];
            SqlType fieldSqlType = seaTunnelDataType.getSqlType();
            if (null == values.get(columnIndex)) {
                seaTunnelField = null;
            } else if (SqlType.BOOLEAN.equals(fieldSqlType)) {
                seaTunnelField = Boolean.parseBoolean(values.get(columnIndex).toString());
            } else if (SqlType.SMALLINT.equals(fieldSqlType)) {
                seaTunnelField = Short.valueOf(values.get(columnIndex).toString());
            } else if (SqlType.INT.equals(fieldSqlType)) {
                seaTunnelField = Integer.valueOf(values.get(columnIndex).toString());
            } else if (SqlType.BIGINT.equals(fieldSqlType)) {
                seaTunnelField = Long.valueOf(values.get(columnIndex).toString());
            } else if (SqlType.FLOAT.equals(fieldSqlType)) {
                seaTunnelField = ((Double) values.get(columnIndex)).floatValue();
            } else if (SqlType.DOUBLE.equals(fieldSqlType)) {
                seaTunnelField = values.get(columnIndex);
            } else if (SqlType.STRING.equals(fieldSqlType)) {
                seaTunnelField = values.get(columnIndex);
            } else {
                throw new InfluxdbConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "Unsupported data type: " + seaTunnelDataType);
            }

            fields.add(seaTunnelField);
        }

        return new SeaTunnelRow(fields.toArray());
    }
}
