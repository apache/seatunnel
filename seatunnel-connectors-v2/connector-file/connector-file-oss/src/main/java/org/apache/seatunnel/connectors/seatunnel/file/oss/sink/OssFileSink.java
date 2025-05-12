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

package org.apache.seatunnel.connectors.seatunnel.file.oss.sink;

import com.typesafe.config.Config;
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
import org.apache.seatunnel.connectors.seatunnel.file.oss.config.OssFileSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.oss.config.OssHadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.BaseMultipleTableFileSink;

import java.util.Optional;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;

public class OssFileSink extends BaseMultipleTableFileSink implements SupportSaveMode {

    private final CatalogTable catalogTable;

    private final ReadonlyConfig readonlyConfig;


    public OssFileSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        super(OssHadoopConf.buildWithConfig(readonlyConfig), readonlyConfig, catalogTable);
        this.catalogTable = catalogTable;
        this.readonlyConfig = readonlyConfig;
        Config pluginConfig = readonlyConfig.toConfig();
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        OssFileSinkOptions.FILE_PATH.key(),
                        OssFileSinkOptions.BUCKET.key());
        if (!result.isSuccess()) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
    }

    @Override
    public String getPluginName() {
        return FileSystemType.OSS.getFileSystemPluginName();
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {

        CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CatalogFactory.class,
                        FileSystemType.OSS.getFileSystemPluginName());
        if (catalogFactory == null) {
            return Optional.empty();
        }
        final Catalog catalog =
                catalogFactory.createCatalog(
                        FileSystemType.OSS.getFileSystemPluginName(), readonlyConfig);
        SchemaSaveMode schemaSaveMode = readonlyConfig.get(OssFileSinkOptions.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = readonlyConfig.get(OssFileSinkOptions.DATA_SAVE_MODE);
        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode, dataSaveMode, catalog, catalogTable, null));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
