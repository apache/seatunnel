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

package org.apache.seatunnel.connectors.seatunnel.starrocks.serialize;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.JsonUtils;

import java.util.LinkedHashMap;
import java.util.Map;

public class StarRocksJsonSerializer extends StarRocksBaseSerializer
        implements StarRocksISerializer {

    private static final long serialVersionUID = 1L;

    /**
     * Parses JSON logical values without converting nested content into quoted strings.
     *
     * <p>The mapper is stateless after configuration and is shared by all serializer instances.
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final SeaTunnelRowType seaTunnelRowType;
    private final boolean enableUpsertDelete;

    public StarRocksJsonSerializer(SeaTunnelRowType seaTunnelRowType, boolean enableUpsertDelete) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.enableUpsertDelete = enableUpsertDelete;
    }

    @Override
    public String serialize(SeaTunnelRow row) {
        Map<String, Object> rowMap = new LinkedHashMap<>(row.getFields().length);

        for (int i = 0; i < row.getFields().length; i++) {
            SqlType sqlType = seaTunnelRowType.getFieldType(i).getSqlType();
            Object value;
            if (sqlType == SqlType.JSON) {
                try {
                    value =
                            row.getField(i) == null
                                    ? null
                                    : readJsonValue((String) row.getField(i));
                } catch (Exception e) {
                    throw CommonError.jsonOperationError("StarRocks", "invalid JSON value", e);
                }
            } else if (sqlType == SqlType.ARRAY
                    || sqlType == SqlType.MAP
                    || sqlType == SqlType.ROW
                    || sqlType == SqlType.MULTIPLE_ROW) {
                // If the field type is complex type, we should keep the origin value.
                // It will be transformed to json string in the next step
                // JsonUtils.toJsonString(rowMap).
                value = row.getField(i);
            } else {
                value = convert(seaTunnelRowType.getFieldType(i), row.getField(i));
            }
            rowMap.put(seaTunnelRowType.getFieldName(i), value);
        }
        if (enableUpsertDelete) {
            rowMap.put(
                    StarRocksSinkOP.COLUMN_KEY, StarRocksSinkOP.parse(row.getRowKind()).ordinal());
        }
        return JsonUtils.toJsonString(rowMap);
    }

    /**
     * Parses one complete JSON value for a native StarRocks JSON column.
     *
     * @param value JSON text represented by the SeaTunnel JSON logical type
     * @return parsed JSON tree retained as structured Stream Load content
     * @throws Exception when the input is empty, malformed, or contains trailing tokens
     */
    private JsonNode readJsonValue(String value) throws Exception {
        JsonNode jsonNode =
                OBJECT_MAPPER
                        .reader()
                        .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                        .readTree(value);
        if (jsonNode == null || jsonNode.isMissingNode()) {
            throw new IllegalArgumentException("JSON value is empty");
        }
        return jsonNode;
    }
}
