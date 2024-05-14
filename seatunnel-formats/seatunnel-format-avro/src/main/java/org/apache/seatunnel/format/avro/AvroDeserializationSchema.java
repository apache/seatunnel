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

    public AvroDeserializationSchema(CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.converter = new AvroToRowConverter(rowType);
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(message, null);
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
