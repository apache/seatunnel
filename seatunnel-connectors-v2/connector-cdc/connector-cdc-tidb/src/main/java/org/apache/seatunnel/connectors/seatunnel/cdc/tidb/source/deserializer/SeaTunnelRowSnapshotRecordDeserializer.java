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

/** Deserialize snapshot data */
@Slf4j
public class SeaTunnelRowSnapshotRecordDeserializer
        extends AbstractSeaTunnelRowDeserializer<Kvrpcpb.KvPair> {

    private final DataConverter converter;

    public SeaTunnelRowSnapshotRecordDeserializer(
            TiTableInfo tableInfo, CatalogTable catalogTable) {
        super(tableInfo, catalogTable);
        this.converter = new DefaultDataConverter();
    }

    @Override
    public void deserialize(Kvrpcpb.KvPair record, Collector<SeaTunnelRow> output)
            throws Exception {
        Object[] values =
                decodeObjects(
                        record.getValue().toByteArray(),
                        RowKey.decode(record.getKey().toByteArray()).getHandle(),
                        tableInfo);
        SeaTunnelRow row = converter.convert(values, tableInfo, rowType);
        output.collect(row);
    }
}
