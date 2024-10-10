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
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.tikv.common.meta.TiTableInfo;

public abstract class AbstractSeaTunnelRowDeserializer<Input> {
    protected final TiTableInfo tableInfo;
    protected final SeaTunnelRowType rowType;
    protected final CatalogTable catalogTable;

    protected AbstractSeaTunnelRowDeserializer(TiTableInfo tableInfo, CatalogTable catalogTable) {
        this.tableInfo = tableInfo;
        this.rowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        this.catalogTable = catalogTable;
    }

    abstract void deserialize(Input record, Collector<SeaTunnelRow> output) throws Exception;
}
