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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.deserializer;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.converter.DataConverter;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.converter.DefaultDataConverter;

import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Cdcpb;

import static org.tikv.common.codec.TableCodec.decodeObjects;

public class SeaTunnelRowStreamingRecordDeserializer
        extends AbstractSeaTunnelRowDeserializer<Cdcpb.Event.Row> {

    private final DataConverter converter;

    public SeaTunnelRowStreamingRecordDeserializer(
            TiTableInfo tableInfo, CatalogTable catalogTable) {
        super(tableInfo, catalogTable);
        converter = new DefaultDataConverter();
    }

    @Override
    public void deserialize(Cdcpb.Event.Row row, Collector<SeaTunnelRow> output) throws Exception {

        final RowKey rowKey = RowKey.decode(row.getKey().toByteArray());
        final long handle = rowKey.getHandle();
        Object[] values;
        switch (row.getOpType()) {
            case DELETE:
                values = decodeObjects(row.getOldValue().toByteArray(), handle, tableInfo);
                SeaTunnelRow record = converter.convert(values, tableInfo, rowType);
                record.setRowKind(RowKind.DELETE);
                output.collect(record);
                break;
            case PUT:
                try {
                    values =
                            decodeObjects(
                                    row.getValue().toByteArray(),
                                    RowKey.decode(row.getKey().toByteArray()).getHandle(),
                                    tableInfo);
                    if (row.getOldValue() == null || row.getOldValue().isEmpty()) {
                        SeaTunnelRow insert = converter.convert(values, tableInfo, rowType);
                        insert.setRowKind(RowKind.INSERT);
                        output.collect(insert);
                    } else {
                        SeaTunnelRow update = converter.convert(values, tableInfo, rowType);
                        update.setRowKind(RowKind.UPDATE_AFTER);
                        output.collect(update);
                    }
                    break;
                } catch (final RuntimeException e) {
                    throw new RuntimeException(
                            String.format(
                                    "Fail to deserialize row: %s, table: %s",
                                    row, tableInfo.getId()),
                            e);
                }
            default:
                throw new IllegalArgumentException("Unknown Row Op Type: " + row.getOpType());
        }
    }
}
