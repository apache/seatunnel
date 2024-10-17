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

package org.apache.seatunnel.connectors.tencent.vectordb.utils;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.BufferUtils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.tencent.tcvectordb.model.DocField;
import com.tencent.tcvectordb.model.Document;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.type.VectorType.VECTOR_FLOAT_TYPE;

public class ConverterUtils {
    public static SeaTunnelRow convertToSeatunnelRow(TableSchema tableSchema, Document vector) {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[typeInfo.getTotalFields()];
        List<String> fieldNames =
                Arrays.stream(typeInfo.getFieldNames()).collect(Collectors.toList());

        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            if (fieldNames.get(fieldIndex).equals("id")) {
                fields[fieldIndex] = vector.getId();
            } else if (fieldNames.get(fieldIndex).equals("meta")) {
                List<DocField> meta = vector.getDocFields();
                JsonObject data = new JsonObject();
                for (DocField entry : meta) {
                    data.add(entry.getName(), convertValueToJsonElement(entry.getValue()));
                }
                fields[fieldIndex] = data;
            } else if (typeInfo.getFieldType(fieldIndex).equals(VECTOR_FLOAT_TYPE)) {
                // Convert each Double to Float
                Float[] arrays =
                        vector.getVector().stream().map(Double::floatValue).toArray(Float[]::new);
                fields[fieldIndex] = BufferUtils.toByteBuffer(arrays);
            }
        }

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        seaTunnelRow.setRowKind(RowKind.INSERT);
        return seaTunnelRow;
    }

    private static JsonElement convertValueToJsonElement(Object value) {
        Gson gson = new Gson();
        return gson.toJsonTree(value);
    }
}
