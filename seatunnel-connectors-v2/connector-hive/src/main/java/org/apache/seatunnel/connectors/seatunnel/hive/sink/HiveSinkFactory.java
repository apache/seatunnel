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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SinkReplaceNameConstant;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

@AutoService(Factory.class)
public class HiveSinkFactory
        implements TableSinkFactory<
                SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo> {

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HiveConfig.TABLE_NAME)
                .required(HiveConfig.METASTORE_URI)
                .optional(HiveConfig.ABORT_DROP_PARTITION_METADATA)
                .optional(BaseSinkConfig.KERBEROS_PRINCIPAL)
                .optional(BaseSinkConfig.KERBEROS_KEYTAB_PATH)
                .optional(BaseSinkConfig.REMOTE_USER)
                .optional(HiveConfig.HADOOP_CONF)
                .optional(HiveConfig.HADOOP_CONF_PATH)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo>
            createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();

        ReadonlyConfig finalReadonlyConfig =
                generateCurrentReadonlyConfig(readonlyConfig, catalogTable);
        return () -> new HiveSink(finalReadonlyConfig, catalogTable);
    }

    @Override
    public String factoryIdentifier() {
        return HiveConstants.CONNECTOR_NAME;
    }

    private ReadonlyConfig generateCurrentReadonlyConfig(
            ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {

        Map<String, String> configMap = readonlyConfig.toMap();

        readonlyConfig
                .getOptional(HiveSinkOptions.TABLE_NAME)
                .ifPresent(
                        tableName -> {
                            String replacedPath =
                                    replaceCatalogTableInPath(tableName, catalogTable);
                            configMap.put(HiveSinkOptions.TABLE_NAME.key(), replacedPath);
                        });

        return ReadonlyConfig.fromMap(new HashMap<>(configMap));
    }

    private String replaceCatalogTableInPath(String originTableName, CatalogTable catalogTable) {
        String tableName = originTableName;
        TableIdentifier tableIdentifier = catalogTable.getTableId();
        if (tableIdentifier != null) {
            if (tableIdentifier.getDatabaseName() != null) {
                tableName =
                        tableName.replace(
                                SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY,
                                tableIdentifier.getDatabaseName());
            }
            if (tableIdentifier.getSchemaName() != null) {
                tableName =
                        tableName.replace(
                                SinkReplaceNameConstant.REPLACE_SCHEMA_NAME_KEY,
                                tableIdentifier.getSchemaName());
            }
            if (tableIdentifier.getTableName() != null) {
                tableName =
                        tableName.replace(
                                SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY,
                                tableIdentifier.getTableName());
            }
        }
        return tableName;
    }
}
