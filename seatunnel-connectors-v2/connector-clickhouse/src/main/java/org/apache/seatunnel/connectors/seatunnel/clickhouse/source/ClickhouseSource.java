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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.clickhouse.client.ClickHouseNode;

import java.util.Collections;
import java.util.List;

public class ClickhouseSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private final List<ClickHouseNode> servers;
    private final CatalogTable catalogTable;
    private final String sql;
    private final SeaTunnelRowType rowTypeInfo;

    public ClickhouseSource(List<ClickHouseNode> servers, CatalogTable catalogTable, String sql) {
        this.servers = servers;
        this.catalogTable = catalogTable;
        this.sql = sql;
        this.rowTypeInfo = catalogTable.getSeaTunnelRowType();
    }

    @Override
    public String getPluginName() {
        return "Clickhouse";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) {
        return new ClickhouseSourceReader(servers, readerContext, sql, rowTypeInfo);
    }
}
