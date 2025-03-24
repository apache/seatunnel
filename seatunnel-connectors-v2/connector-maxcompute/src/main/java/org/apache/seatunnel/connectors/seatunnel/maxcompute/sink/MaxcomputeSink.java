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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.sink;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.catalog.MaxComputeCatalog;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.exception.MaxcomputeConnectorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.OVERWRITE;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PLUGIN_NAME;

public class MaxcomputeSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportSaveMode, SupportMultiTableSink {
    private static final Logger LOG = LoggerFactory.getLogger(MaxcomputeSink.class);
    private final ReadonlyConfig readonlyConfig;
    private final CatalogTable catalogTable;

    public MaxcomputeSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.readonlyConfig = readonlyConfig;
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public MaxcomputeWriter createWriter(SinkWriter.Context context) {
        return new MaxcomputeWriter(this.readonlyConfig, this.catalogTable.getSeaTunnelRowType());
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CatalogFactory.class,
                        "MaxCompute");
        if (catalogFactory == null) {
            throw new MaxcomputeConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(),
                            PluginType.SINK,
                            "Cannot find MaxCompute catalog factory"));
        }
        MaxComputeCatalog catalog =
                (MaxComputeCatalog)
                        catalogFactory.createCatalog(
                                catalogFactory.factoryIdentifier(), readonlyConfig);

        DataSaveMode dataSaveMode = readonlyConfig.get(MaxcomputeConfig.DATA_SAVE_MODE);
        if (readonlyConfig.get(OVERWRITE)) {
            // compatible with old version
            LOG.warn(
                    "The configuration of 'overwrite' is deprecated, please use 'data_save_mode' instead.");
            dataSaveMode = DataSaveMode.DROP_DATA;
        }

        return Optional.of(
                new MaxComputeSaveModeHandler(
                        readonlyConfig.get(MaxcomputeConfig.SCHEMA_SAVE_MODE),
                        dataSaveMode,
                        catalog,
                        catalogTable,
                        readonlyConfig.get(MaxcomputeConfig.CUSTOM_SQL),
                        readonlyConfig));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return super.getWriteCatalogTable();
    }
}
