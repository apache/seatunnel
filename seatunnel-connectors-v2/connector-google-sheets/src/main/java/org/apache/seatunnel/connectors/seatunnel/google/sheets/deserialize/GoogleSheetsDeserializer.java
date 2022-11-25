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

package org.apache.seatunnel.connectors.seatunnel.google.sheets.deserialize;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.exception.GoogleSheetsConnectorException;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GoogleSheetsDeserializer implements SeaTunnelRowDeserializer {

    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String[] fields;

    public GoogleSheetsDeserializer(String[] fields, DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.fields = fields;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public SeaTunnelRow deserializeRow(List<Object> row) {
        try {
            Map<String, Object> map = new HashMap<>();
            for (int i = 0; i < row.size(); i++) {
                if (i < fields.length) {
                    map.put(fields[i], row.get(i));
                }
            }
            String rowStr = objectMapper.writeValueAsString(map);
            return deserializationSchema.deserialize(rowStr.getBytes());
        } catch (IOException e) {
            throw new GoogleSheetsConnectorException(CommonErrorCode.JSON_OPERATION_FAILED,
                "Object json deserialization failed.", e);
        }
    }
}
