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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.converter.DataConverter;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.converter.DefaultDataConverter;

import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Kvrpcpb;

import lombok.extern.slf4j.Slf4j;

import static org.tikv.common.codec.TableCodec.decodeObjects;

/**
 * Decodes snapshot row images and restores missing clustered primary-key values from record keys.
 * Integer handles are decoded only for tables whose metadata declares the integer PK layout.
 */
@Slf4j
public class SeaTunnelRowSnapshotRecordDeserializer
        extends AbstractSeaTunnelRowDeserializer<Kvrpcpb.KvPair> {

    private final DataConverter converter;
    private final boolean clusteredPrimaryKey;

    public SeaTunnelRowSnapshotRecordDeserializer(
            TiTableInfo tableInfo, CatalogTable catalogTable) {
        this(tableInfo, catalogTable, false);
    }

    /**
     * Uses explicit table metadata to distinguish common handles from non-clustered row IDs.
     * Common-handle bytes must never be decoded as an integer before restoring omitted columns.
     */
    public SeaTunnelRowSnapshotRecordDeserializer(
            TiTableInfo tableInfo, CatalogTable catalogTable, boolean clusteredPrimaryKey) {
        super(tableInfo, catalogTable);
        this.clusteredPrimaryKey = clusteredPrimaryKey;
        this.converter = new DefaultDataConverter();
    }

    @Override
    public void deserialize(Kvrpcpb.KvPair record, Collector<SeaTunnelRow> output)
            throws Exception {
        byte[] key = record.getKey().toByteArray();
        Object[] values =
                decodeObjects(
                        record.getValue().toByteArray(),
                        tableInfo.isPkHandle() ? RowKey.decode(key).getHandle() : null,
                        tableInfo);
        CommonHandleDecoder.restorePrimaryKeyColumns(key, values, tableInfo, clusteredPrimaryKey);
        SeaTunnelRow row = converter.convert(values, tableInfo, rowType);
        collect(row, output);
    }
}
