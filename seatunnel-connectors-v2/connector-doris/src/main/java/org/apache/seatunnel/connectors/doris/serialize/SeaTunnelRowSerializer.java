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

package org.apache.seatunnel.connectors.doris.serialize;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.sink.writer.LoadConstants;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.connectors.doris.sink.writer.LoadConstants.CSV;
import static org.apache.seatunnel.connectors.doris.sink.writer.LoadConstants.JSON;
import static org.apache.seatunnel.connectors.doris.sink.writer.LoadConstants.NULL_VALUE;

public class SeaTunnelRowSerializer implements DorisSerializer {
    String type;
    private final SeaTunnelRowType seaTunnelRowType;
    private final String fieldDelimiter;
    private final boolean enableDelete;
    private final SerializationSchema serialize;

    public SeaTunnelRowSerializer(
            String type,
            SeaTunnelRowType seaTunnelRowType,
            String fieldDelimiter,
            boolean enableDelete) {
        this.type = type;
        this.fieldDelimiter = fieldDelimiter;
        this.enableDelete = enableDelete;
        List<Object> fieldNames = new ArrayList<>(Arrays.asList(seaTunnelRowType.getFieldNames()));
        List<SeaTunnelDataType<?>> fieldTypes =
                new ArrayList<>(Arrays.asList(seaTunnelRowType.getFieldTypes()));

        if (enableDelete) {
            fieldNames.add(LoadConstants.DORIS_DELETE_SIGN);
            fieldTypes.add(STRING_TYPE);
        }

        this.seaTunnelRowType =
                new SeaTunnelRowType(
                        fieldNames.toArray(new String[0]),
                        fieldTypes.toArray(new SeaTunnelDataType<?>[0]));

        if (JSON.equals(type)) {
            JsonSerializationSchema jsonSerializationSchema =
                    new JsonSerializationSchema(this.seaTunnelRowType);
            ObjectMapper mapper = jsonSerializationSchema.getMapper();
            mapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
            this.serialize = jsonSerializationSchema;
        } else {
            this.serialize =
                    TextSerializationSchema.builder()
                            .seaTunnelRowType(this.seaTunnelRowType)
                            .delimiter(fieldDelimiter)
                            .nullValue(NULL_VALUE)
                            .build();
        }
    }

    public byte[] buildJsonString(SeaTunnelRow row) {

        return serialize.serialize(row);
    }

    public byte[] buildCSVString(SeaTunnelRow row) {

        return serialize.serialize(row);
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

    @Override
    public void open() throws IOException {}

    @Override
    public byte[] serialize(SeaTunnelRow seaTunnelRow) throws IOException {

        if (enableDelete) {

            List<Object> newFields = new ArrayList<>(Arrays.asList(seaTunnelRow.getFields()));
            newFields.add(parseDeleteSign(seaTunnelRow.getRowKind()));
            seaTunnelRow = new SeaTunnelRow(newFields.toArray());
        }

        if (JSON.equals(type)) {
            return buildJsonString(seaTunnelRow);
        } else if (CSV.equals(type)) {
            return buildCSVString(seaTunnelRow);
        } else {
            throw new IllegalArgumentException("The type " + type + " is not supported!");
        }
    }

    @Override
    public void close() throws IOException {}
}
