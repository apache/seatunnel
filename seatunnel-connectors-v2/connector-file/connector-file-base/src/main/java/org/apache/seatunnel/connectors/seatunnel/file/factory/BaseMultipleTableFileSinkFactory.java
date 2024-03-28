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

package org.apache.seatunnel.connectors.seatunnel.file.factory;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkReplaceNameConstant;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;

public abstract class BaseMultipleTableFileSinkFactory
        implements TableSinkFactory<
                SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo> {

    // replace the table name in sink config's path
    public ReadonlyConfig generateCurrentReadonlyConfig(
            ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        // Copy the config to avoid modifying the original config
        Config config = readonlyConfig.toConfig();

        if (config.hasPath(BaseSinkConfig.FILE_PATH.key())) {
            String replacedPath =
                    replaceCatalogTableInPath(
                            config.getString(BaseSinkConfig.FILE_PATH.key()), catalogTable);
            config =
                    config.withValue(
                            BaseSinkConfig.FILE_PATH.key(),
                            ConfigValueFactory.fromAnyRef(replacedPath));
        }

        if (config.hasPath(BaseSinkConfig.TMP_PATH.key())) {
            String replacedPath =
                    replaceCatalogTableInPath(
                            config.getString(BaseSinkConfig.TMP_PATH.key()), catalogTable);
            config =
                    config.withValue(
                            BaseSinkConfig.TMP_PATH.key(),
                            ConfigValueFactory.fromAnyRef(replacedPath));
        }

        return ReadonlyConfig.fromConfig(config);
    }

    public String replaceCatalogTableInPath(String originString, CatalogTable catalogTable) {
        String path = originString;
        TableIdentifier tableIdentifier = catalogTable.getTableId();
        if (tableIdentifier != null) {
            if (tableIdentifier.getDatabaseName() != null) {
                path =
                        path.replace(
                                SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY,
                                tableIdentifier.getDatabaseName());
            }
            if (tableIdentifier.getSchemaName() != null) {
                path =
                        path.replace(
                                SinkReplaceNameConstant.REPLACE_SCHEMA_NAME_KEY,
                                tableIdentifier.getSchemaName());
            }
            if (tableIdentifier.getTableName() != null) {
                path =
                        path.replace(
                                SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY,
                                tableIdentifier.getTableName());
            }
        }
        return path;
    }
}
