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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.ArrayList;
import java.util.List;

public class InfluxDBRowConverter {

    public static SeaTunnelRow convert(List<Object> values, SeaTunnelRowType typeInfo, List<Integer> indexList) {

        List<Object> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = typeInfo.getFieldTypes();

        for (int i = 0; i <= seaTunnelDataTypes.length - 1; i++) {
            Object seatunnelField;
            int columnIndex = indexList.get(i);
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[i];
            if (null == values.get(columnIndex)) {
                seatunnelField = null;
            }
            else if (BasicType.BOOLEAN_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = Boolean.parseBoolean(values.get(columnIndex).toString());
            }  else if (BasicType.INT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = (Integer) values.get(columnIndex);
            } else if (BasicType.LONG_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = (Long) values.get(columnIndex);
            } else if (BasicType.FLOAT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = ((Double) values.get(columnIndex)).floatValue();
            } else if (BasicType.DOUBLE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = (Double) values.get(columnIndex);
            } else if (PrimitiveByteArrayType.INSTANCE.equals(seaTunnelDataType)) {
                seatunnelField = (byte[]) values.get(i);
            } else if (BasicType.STRING_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = values.get(columnIndex);
            } else {
                throw new IllegalStateException("Unexpected value: " + seaTunnelDataType);
            }

            fields.add(seatunnelField);
        }

        return new SeaTunnelRow(fields.toArray());
    }
}
