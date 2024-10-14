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

package org.apache.seatunnel.connectors.seatunnel.timeplus.sink;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
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
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.timeplus.config.ReaderOption;
import org.apache.seatunnel.connectors.seatunnel.timeplus.exception.TimeplusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.client.TimeplusSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.timeplus.state.TPAggCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.timeplus.state.TPCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.timeplus.state.TimeplusSinkState;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;
import static org.icecream.IceCream.ic;

public class TimeplusSink
        implements SeaTunnelSink<SeaTunnelRow, TimeplusSinkState, TPCommitInfo, TPAggCommitInfo>,
                SupportSaveMode,
                SupportMultiTableSink {

    private ReaderOption option;

    private ReadonlyConfig readonlyConfig;

    private CatalogTable catalogTable;

    private DataSaveMode dataSaveMode;
    private SchemaSaveMode schemaSaveMode;

    @Override
    public String getPluginName() {
        return "Timeplus";
    }

    public TimeplusSink(
            CatalogTable catalogTable, ReaderOption option, ReadonlyConfig readonlyConfig) {
        ic("new TimeplusSink with catalog table", catalogTable, option, readonlyConfig);
        this.catalogTable = catalogTable;
        this.option = option;
        this.dataSaveMode = option.getDataSaveMode();
        this.schemaSaveMode = option.getSchemaSaveMode();
        this.readonlyConfig = readonlyConfig;
    }

    @Override
    public TimeplusSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return new TimeplusSinkWriter(option, context, readonlyConfig);
    }

    @Override
    public SinkWriter<SeaTunnelRow, TPCommitInfo, TimeplusSinkState> restoreWriter(
            SinkWriter.Context context, List<TimeplusSinkState> states) throws IOException {
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    @Override
    public Optional<Serializer<TimeplusSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName("com.timeplus.proton.jdbc.ProtonDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CatalogFactory.class,
                        "Timeplus");
        if (catalogFactory == null) {
            throw new TimeplusConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(),
                            PluginType.SINK,
                            "Cannot find Timeplus catalog factory"));
        }

        Catalog catalog =
                catalogFactory.createCatalog(catalogFactory.factoryIdentifier(), readonlyConfig);
        catalog.open();
        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode, dataSaveMode, catalog, catalogTable, null));
    }
}
