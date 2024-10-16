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

package org.apache.seatunnel.connectors.seatunnel.hbase.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.connectors.seatunnel.hbase.constant.HbaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.hbase.state.HbaseAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hbase.state.HbaseCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hbase.state.HbaseSinkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;

public class HbaseSink
        implements SeaTunnelSink<
                        SeaTunnelRow, HbaseSinkState, HbaseCommitInfo, HbaseAggregatedCommitInfo>,
                SupportMultiTableSink,
                SupportSaveMode {

    private final ReadonlyConfig config;

    private final CatalogTable catalogTable;

    private final HbaseParameters hbaseParameters;

    private final SeaTunnelRowType seaTunnelRowType;

    private final List<Integer> rowkeyColumnIndexes = new ArrayList<>();

    private int versionColumnIndex = -1;

    public HbaseSink(ReadonlyConfig config, CatalogTable catalogTable) {
        this.hbaseParameters = HbaseParameters.buildWithConfig(config);
        this.config = config;
        this.catalogTable = catalogTable;
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        if (hbaseParameters.getVersionColumn() != null) {
            this.versionColumnIndex = seaTunnelRowType.indexOf(hbaseParameters.getVersionColumn());
        }
    }

    @Override
    public String getPluginName() {
        return HbaseIdentifier.IDENTIFIER_NAME;
    }

    @Override
    public HbaseSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        for (String rowkeyColumn : hbaseParameters.getRowkeyColumns()) {
            this.rowkeyColumnIndexes.add(seaTunnelRowType.indexOf(rowkeyColumn));
        }
        if (hbaseParameters.getVersionColumn() != null) {
            this.versionColumnIndex = seaTunnelRowType.indexOf(hbaseParameters.getVersionColumn());
        }
        return new HbaseSinkWriter(
                seaTunnelRowType, hbaseParameters, rowkeyColumnIndexes, versionColumnIndex);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CatalogFactory.class,
                        getPluginName());
        if (catalogFactory == null) {
            return Optional.empty();
        }
        Catalog catalog = catalogFactory.createCatalog(catalogFactory.factoryIdentifier(), config);
        SchemaSaveMode schemaSaveMode = config.get(HbaseConfig.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = config.get(HbaseConfig.DATA_SAVE_MODE);
        TablePath tablePath =
                TablePath.of(hbaseParameters.getNamespace(), hbaseParameters.getTable());
        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode, dataSaveMode, catalog, tablePath, null, null));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
