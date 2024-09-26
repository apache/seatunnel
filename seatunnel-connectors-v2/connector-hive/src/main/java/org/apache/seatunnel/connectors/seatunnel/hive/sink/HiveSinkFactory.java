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
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;

import com.google.auto.service.AutoService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@AutoService(Factory.class)
public class HiveSinkFactory
        implements TableSinkFactory<
                SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo> {

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HiveConfig.TABLE_NAME, HiveConfig.METASTORE_URI)
                .optional(HiveConfig.HIVE_JDBC_URL)
                .optional(HiveConfig.ABORT_DROP_PARTITION_METADATA)
                .optional(HiveConfig.HADOOP_CONF)
                .optional(HiveConfig.HADOOP_CONF_PATH)
                .optional(HiveSinkOptions.KERBEROS_PRINCIPAL)
                .optional(HiveSinkOptions.KERBEROS_KEYTAB_PATH)
                .optional(HiveSinkOptions.REMOTE_USER)
                .optional(HiveSinkOptions.SCHEMA_SAVE_MODE)
                .optional(HiveSinkOptions.DATA_SAVE_MODE)
                .optional(HiveSinkOptions.SAVE_MODE_PARTITION_KEYS)
                .conditional(
                        HiveSinkOptions.SCHEMA_SAVE_MODE,
                        Arrays.asList(
                                SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                                SchemaSaveMode.RECREATE_SCHEMA),
                        HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo>
            createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = generateCatalogTable(readonlyConfig, context.getCatalogTable());
        return () -> new HiveSink(readonlyConfig, catalogTable);
    }

    @Override
    public String factoryIdentifier() {
        return HiveConstants.CONNECTOR_NAME;
    }

    private CatalogTable generateCatalogTable(
            ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        String fullTableName = readonlyConfig.get(HiveConfig.TABLE_NAME);
        String databaseName = fullTableName.split("\\.")[0];
        String tableName = fullTableName.split("\\.")[1];
        TableIdentifier newTableId = TableIdentifier.of("Hive", databaseName, null, tableName);

        return CatalogTable.of(newTableId, catalogTable);
    }

    @Override
    public List<String> excludeTablePlaceholderReplaceKeys() {
        return Collections.singletonList("save_mode_create_template");
    }
}
