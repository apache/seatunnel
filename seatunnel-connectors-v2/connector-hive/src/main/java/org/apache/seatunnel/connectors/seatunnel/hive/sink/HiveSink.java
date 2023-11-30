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

package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.schema.ReadonlyConfigParser;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.sink.BaseHdfsFileSink;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hive.catalog.HiveCatalog;
import org.apache.seatunnel.connectors.seatunnel.hive.catalog.HiveCatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.hive.commit.HiveSinkAggregatedCommitter;

import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.CUSTOM_SQL;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.DATA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.SCHEMA_SAVE_MODE;

public class HiveSink extends BaseHdfsFileSink implements SupportSaveMode {
    private String dbName;
    private String tableName;
    private CatalogTable catalogTable;

    public HiveSink(
            String dbName,
            String tableName,
            CatalogTable catalogTable,
            Config pluginConfig,
            HadoopConf hadoopConf) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.catalogTable = catalogTable;
        super.pluginConfig = pluginConfig;
        super.hadoopConf = hadoopConf;
    }

    @Override
    public String getPluginName() {
        return HiveCatalogUtils.CATALOG_NAME;
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>>
            createAggregatedCommitter() {
        return Optional.of(
                new HiveSinkAggregatedCommitter(pluginConfig, dbName, tableName, hadoopConf));
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        if (pluginConfig.hasPath(SCHEMA_SAVE_MODE.key())
                || pluginConfig.hasPath(DATA_SAVE_MODE.key())) {
            ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
            SchemaSaveMode schemaSaveMode = readonlyConfig.get(SCHEMA_SAVE_MODE);
            DataSaveMode dataSaveMode = readonlyConfig.get(DATA_SAVE_MODE);
            String custom_sql = readonlyConfig.get(CUSTOM_SQL);
            new ReadonlyConfigParser().parse(readonlyConfig);
            HiveCatalog catalog = new HiveCatalog(pluginConfig);
            return Optional.of(
                    new DefaultSaveModeHandler(
                            schemaSaveMode, dataSaveMode, catalog, catalogTable, custom_sql));
        }
        return Optional.empty();
    }
}
