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

package org.apache.seatunnel.format.avro;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.util.Optional;

public class AvroDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private static final long serialVersionUID = -7907358485475741366L;

    private final SeaTunnelRowType rowType;
    private final AvroToRowConverter converter;
    private final CatalogTable catalogTable;
    private final boolean stripSchemaRegistryHeader;

    public AvroDeserializationSchema(CatalogTable catalogTable) {
        this(catalogTable, null, false);
    }

    public AvroDeserializationSchema(CatalogTable catalogTable, String writerSchema) {
        this(catalogTable, writerSchema, false);
    }

    public AvroDeserializationSchema(
            CatalogTable catalogTable, String writerSchema, boolean stripSchemaRegistryHeader) {
        this(catalogTable, writerSchema, stripSchemaRegistryHeader, false);
    }

    /**
     * Creates a deserialization schema with the opt-in nullable ROW layout.
     *
     * <p>When {@code writerSchema} is supplied it stays authoritative for the wire layout and
     * {@code rowNullable} has no effect, because the reader schema must match the schema the
     * message was written with. Otherwise the reader schema is derived from the row type, and
     * {@code rowNullable} must match the writer side for ROW fields to be read back correctly.
     *
     * @param catalogTable catalog table of the messages
     * @param writerSchema schema the message was written with, or null to derive it from the row
     *     type
     * @param stripSchemaRegistryHeader whether a Confluent Schema Registry header is stripped
     * @param rowNullable whether a ROW value was written as a nullable Avro union, see {@link
     *     AvroFormatOptions#AVRO_ROW_NULLABLE}
     */
    public AvroDeserializationSchema(
            CatalogTable catalogTable,
            String writerSchema,
            boolean stripSchemaRegistryHeader,
            boolean rowNullable) {
        this.catalogTable = catalogTable;
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.converter = new AvroToRowConverter(rowType, writerSchema, rowNullable);
        this.stripSchemaRegistryHeader = stripSchemaRegistryHeader;
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            // skip tombstone messages
            return null;
        }
        if (stripSchemaRegistryHeader && hasSchemaRegistryHeader(message)) {
            try {
                return deserializePayload(message, 5, message.length - 5);
            } catch (IOException | RuntimeException framedFailure) {
                // A raw Avro payload can legally begin with zero. Preserve compatibility by
                // retrying the complete message when the framed interpretation is not decodable.
            }
        }
        return deserializePayload(message, 0, message.length);
    }

    private static boolean hasSchemaRegistryHeader(byte[] message) {
        return message != null && message.length >= 5 && message[0] == 0;
    }

    private SeaTunnelRow deserializePayload(byte[] message, int offset, int length)
            throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(message, offset, length, null);
        GenericRecord record = this.converter.getReader().read(null, decoder);
        SeaTunnelRow seaTunnelRow = converter.converter(record, rowType);
        Optional<TablePath> tablePath =
                Optional.ofNullable(catalogTable).map(CatalogTable::getTablePath);
        if (tablePath.isPresent()) {
            seaTunnelRow.setTableId(tablePath.toString());
        }
        return seaTunnelRow;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }
}
