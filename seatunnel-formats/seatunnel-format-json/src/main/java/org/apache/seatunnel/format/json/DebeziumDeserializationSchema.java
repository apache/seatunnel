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

package org.apache.seatunnel.format.json;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import java.io.IOException;

public class DebeziumDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    private static final long serialVersionUID = 1L;

    private static final String OP_READ = "r"; // snapshot read
    private static final String OP_CREATE = "c"; // insert
    private static final String OP_UPDATE = "u"; // update
    private static final String OP_DELETE = "d"; // delete

    private static final String REPLICA_IDENTITY_EXCEPTION =
            "The \"before\" field of %s message is null, "
                    + "if you are using Debezium Postgres Connector, "
                    + "please check the Postgres table has been set REPLICA IDENTITY to FULL level.";

    private final SeaTunnelRowType rowType;

    private final JsonDeserializationSchema jsonDeserializer;

    /**
     * Flag indicating whether the Debezium JSON data contains schema part or not. When Debezium
     * Kafka Connect enables "value.converter.schemas.enable", the JSON will contain "schema"
     * information, but we just ignore "schema" and extract data from "payload".
     */
    private final boolean schemaInclude;

    private final boolean ignoreParseErrors;

    public DebeziumDeserializationSchema(
            SeaTunnelRowType rowType,
            boolean schemaInclude,
            boolean ignoreParseErrors) {
        this.rowType = rowType;
        this.schemaInclude = schemaInclude;
        this.ignoreParseErrors = ignoreParseErrors;
        this.jsonDeserializer = new JsonDeserializationSchema(
                false,
                ignoreParseErrors,
                createJsonRowType(rowType, schemaInclude)
        );
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        throw new UnsupportedOperationException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<SeaTunnelRow>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        if (message == null || message.length == 0) {
            // skip tombstone messages
            return;
        }

        try {
            SeaTunnelRow row = jsonDeserializer.deserialize(message);
            SeaTunnelRow payload;
            if (schemaInclude) {
                payload = (SeaTunnelRow) row.getField(0);
            } else {
                payload = row;
            }

            SeaTunnelRow before = (SeaTunnelRow) payload.getField(0);
            SeaTunnelRow after = (SeaTunnelRow) payload.getField(1);
            String op = payload.getField(2).toString();
            if (OP_CREATE.equals(op) || OP_READ.equals(op)) {
                after.setRowKind(RowKind.INSERT);
                out.collect(after);
            } else if (OP_UPDATE.equals(op)) {
                if (before == null) {
                    throw new SeaTunnelJsonFormatException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
                }
                before.setRowKind(RowKind.UPDATE_BEFORE);
                after.setRowKind(RowKind.UPDATE_AFTER);
                out.collect(before);
                out.collect(after);
            } else if (OP_DELETE.equals(op)) {
                if (before == null) {
                    throw new SeaTunnelJsonFormatException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, String.format(REPLICA_IDENTITY_EXCEPTION, "DELETE"));
                }
                before.setRowKind(RowKind.DELETE);
                out.collect(before);
            } else {
                if (!ignoreParseErrors) {
                    throw new SeaTunnelJsonFormatException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, String.format(
                            "Unknown \"op\" value \"%s\". The Debezium JSON message is '%s'", op, new String(message)));
                }
            }
        } catch (Throwable t) {
            // a big try catch to protect the processing.
            if (!ignoreParseErrors) {
                throw new SeaTunnelJsonFormatException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, String.format(
                        "Corrupt Debezium JSON message '%s'.", new String(message)), t);
            }
        }
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }

    private static SeaTunnelRowType createJsonRowType(SeaTunnelRowType databaseSchema, boolean schemaInclude) {
        SeaTunnelRowType payload = new SeaTunnelRowType(new String[]{"before", "after", "op"},
                        new SeaTunnelDataType[]{databaseSchema, databaseSchema, STRING_TYPE});
        if (schemaInclude) {
            // when Debezium Kafka connect enables "value.converter.schemas.enable",
            // the JSON will contain "schema" information, but we just ignore "schema"
            // and extract data from "payload".
            return new SeaTunnelRowType(new String[]{"payload"},
                    new SeaTunnelDataType[]{payload});
        } else {
            // payload contains some other information, e.g. "source", "ts_ms"
            // but we don't need them.
            return payload;
        }
    }
}
