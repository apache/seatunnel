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

package org.apache.seatunnel.connectors.seatunnel.file.s3.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3FileSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.BaseMultipleTableFileSink;

import java.util.Optional;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;

public class S3FileSink extends BaseMultipleTableFileSink implements SupportSaveMode {

    private final CatalogTable catalogTable;
    private final ReadonlyConfig readonlyConfig;

    private static final String S3 = "S3";

    @Override
    public String getPluginName() {
        return FileSystemType.S3.getFileSystemPluginName();
    }

    public S3FileSink(CatalogTable catalogTable, ReadonlyConfig readonlyConfig) {
        super(S3HadoopConf.buildWithReadOnlyConfig(readonlyConfig), readonlyConfig, catalogTable);
        this.catalogTable = catalogTable;
        this.readonlyConfig = readonlyConfig;
        Config pluginConfig = readonlyConfig.toConfig();
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        S3FileSinkOptions.FILE_PATH.key(),
                        S3FileSinkOptions.S3_BUCKET.key());
        if (!result.isSuccess()) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {

        CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(), CatalogFactory.class, S3);
        if (catalogFactory == null) {
            return Optional.empty();
        }
        final Catalog catalog = catalogFactory.createCatalog(S3, readonlyConfig);
        SchemaSaveMode schemaSaveMode = readonlyConfig.get(S3FileSinkOptions.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = readonlyConfig.get(S3FileSinkOptions.DATA_SAVE_MODE);
        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode, dataSaveMode, catalog, catalogTable, null));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
