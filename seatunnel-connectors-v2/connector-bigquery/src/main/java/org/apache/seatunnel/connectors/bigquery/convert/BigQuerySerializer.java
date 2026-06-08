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

package org.apache.seatunnel.connectors.bigquery.convert;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import org.json.JSONObject;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryStreamWriter.CHANGE_TYPE;
import static org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryStreamWriter.SEQUENCE_NUM;

@Slf4j
public class BigQuerySerializer {
    private final JsonSerializationSchema jsonSerializationSchema;
    private final List<String> byteFieldNames = new ArrayList<>();
    private int sequenceFieldIndex = -1;

    public BigQuerySerializer(CatalogTable catalogTable, ReadonlyConfig config) {
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        initialize(rowType, config);
        this.jsonSerializationSchema = new JsonSerializationSchema(rowType);
    }

    private void initialize(SeaTunnelRowType rowType, ReadonlyConfig config) {
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            if (SqlType.BYTES.equals(rowType.getFieldTypes()[i].getSqlType())) {
                byteFieldNames.add(rowType.getFieldName(i));
            }
        }

        String sequenceFieldName = config.get(BigQuerySinkOptions.SEQUENCE_NUMBER_COLUMN);
        if (sequenceFieldName == null) return;

        for (int i = 0; i < rowType.getTotalFields(); i++) {
            if (sequenceFieldName.equals(rowType.getFieldName(i))) {
                this.sequenceFieldIndex = i;
            }
        }
        if (this.sequenceFieldIndex == -1) {
            throw CommonError.illegalArgument(
                    sequenceFieldName, BigQuerySinkOptions.SEQUENCE_NUMBER_COLUMN.key());
        }
    }

    public JSONObject convert(SeaTunnelRow element, boolean includeChangeType) {
        JsonNode jsonNode = jsonSerializationSchema.convert(element);

        JSONObject jsonObject = new JSONObject(jsonNode.toString());
        for (String fieldName : byteFieldNames) {
            JsonNode node = jsonNode.get(fieldName);
            if (node != null && !node.isNull()) {
                byte[] decodedBytes = Base64.getDecoder().decode(node.asText());
                jsonObject.put(fieldName, ByteString.copyFrom(decodedBytes));
            }
        }

        if (includeChangeType) {
            switch (element.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    jsonObject.put(CHANGE_TYPE, "UPSERT");
                    break;
                case DELETE:
                case UPDATE_BEFORE:
                    jsonObject.put(CHANGE_TYPE, "DELETE");
                    break;
                default:
                    throw CommonError.unsupportedOperation(
                            element.getRowKind().toString(), "Unsupported RowKind");
            }
            if (sequenceFieldIndex != -1) {
                jsonObject.put(SEQUENCE_NUM, element.getField(sequenceFieldIndex));
            }
        }

        return jsonObject;
    }
}
