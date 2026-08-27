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

package org.apache.seatunnel.connectors.selectdb.serialize;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.selectdb.sink.writer.LoadConstants;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import static org.apache.seatunnel.connectors.selectdb.sink.writer.LoadConstants.CSV;
import static org.apache.seatunnel.connectors.selectdb.sink.writer.LoadConstants.JSON;
import static org.apache.seatunnel.connectors.selectdb.sink.writer.LoadConstants.NULL_VALUE;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkState;

public class SeaTunnelRowSerializer extends SeaTunnelRowConverter implements SelectDBSerializer {
    String type;
    private ObjectMapper objectMapper;
    private final SeaTunnelRowType seaTunnelRowType;
    private final String fieldDelimiter;
    private final boolean enableDelete;

    public SeaTunnelRowSerializer(
            String type,
            SeaTunnelRowType seaTunnelRowType,
            String fieldDelimiter,
            boolean enableDelete) {
        this.type = type;
        this.seaTunnelRowType = seaTunnelRowType;
        this.fieldDelimiter = fieldDelimiter;
        this.enableDelete = enableDelete;
        if (JSON.equals(type)) {
            objectMapper = new ObjectMapper();
        }
    }

    @Override
    public byte[] serialize(SeaTunnelRow seaTunnelRow) throws IOException {
        String valString;
        if (JSON.equals(type)) {
            valString = buildJsonString(seaTunnelRow);
        } else if (CSV.equals(type)) {
            valString = buildCSVString(seaTunnelRow);
        } else {
            throw new IllegalArgumentException("The type " + type + " is not supported!");
        }
        return valString.getBytes(StandardCharsets.UTF_8);
    }

    public String buildJsonString(SeaTunnelRow row) throws IOException {
        Map<String, Object> rowMap = new HashMap<>(row.getFields().length);

        for (int i = 0; i < row.getFields().length; i++) {
            Object value = convert(seaTunnelRowType.getFieldType(i), row.getField(i));
            rowMap.put(seaTunnelRowType.getFieldName(i), value);
        }
        if (enableDelete) {
            rowMap.put(LoadConstants.DORIS_DELETE_SIGN, parseDeleteSign(row.getRowKind()));
        }
        return objectMapper.writeValueAsString(rowMap);
    }

    public String buildCSVString(SeaTunnelRow row) throws IOException {
        StringJoiner joiner = new StringJoiner(fieldDelimiter);
        for (int i = 0; i < row.getFields().length; i++) {
            Object field = convert(seaTunnelRowType.getFieldType(i), row.getField(i));
            String value = field != null ? field.toString() : NULL_VALUE;
            joiner.add(value);
        }
        if (enableDelete) {
            joiner.add(parseDeleteSign(row.getRowKind()));
        }
        return joiner.toString();
    }

    public String parseDeleteSign(RowKind rowKind) {
        if (RowKind.INSERT.equals(rowKind) || RowKind.UPDATE_AFTER.equals(rowKind)) {
            return "0";
        } else if (RowKind.DELETE.equals(rowKind) || RowKind.UPDATE_BEFORE.equals(rowKind)) {
            return "1";
        } else {
            throw new IllegalArgumentException("Unrecognized row kind:" + rowKind.toString());
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for RowDataSerializer. */
    public static class Builder {
        private SeaTunnelRowType seaTunnelRowType;
        private String type;
        private String fieldDelimiter;
        private boolean deletable;

        public Builder setType(String type) {
            this.type = type;
            return this;
        }

        public Builder setSeaTunnelRowType(SeaTunnelRowType seaTunnelRowType) {
            this.seaTunnelRowType = seaTunnelRowType;
            return this;
        }

        public Builder setFieldDelimiter(String fieldDelimiter) {
            this.fieldDelimiter = fieldDelimiter;
            return this;
        }

        public Builder enableDelete(boolean deletable) {
            this.deletable = deletable;
            return this;
        }

        public SeaTunnelRowSerializer build() {
            checkState(CSV.equals(type) && fieldDelimiter != null || JSON.equals(type));
            return new SeaTunnelRowSerializer(type, seaTunnelRowType, fieldDelimiter, deletable);
        }
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {}
}
