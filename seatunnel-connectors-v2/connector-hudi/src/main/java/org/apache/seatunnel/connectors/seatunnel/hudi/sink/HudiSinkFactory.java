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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.CONF_FILES_PATH;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.TABLE_DFS_PATH;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.TABLE_LIST;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.BATCH_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.CDC_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.INDEX_CLASS_NAME;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.INDEX_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.INSERT_SHUFFLE_PARALLELISM;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.MAX_COMMITS_TO_KEEP;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.MIN_COMMITS_TO_KEEP;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.OP_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.PARTITION_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.RECORD_BYTE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.RECORD_KEY_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.TABLE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.TABLE_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.UPSERT_SHUFFLE_PARALLELISM;
import static org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiErrorCode.TABLE_CONFIG_NOT_FOUND;

@AutoService(Factory.class)
public class HudiSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Hudi";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(
                        TABLE_NAME,
                        TABLE_DFS_PATH,
                        TABLE_TYPE,
                        RECORD_KEY_FIELDS,
                        PARTITION_FIELDS,
                        INDEX_TYPE,
                        INDEX_CLASS_NAME,
                        RECORD_BYTE_SIZE,
                        TABLE_LIST,
                        CONF_FILES_PATH,
                        OP_TYPE,
                        BATCH_SIZE,
                        BATCH_INTERVAL_MS,
                        INSERT_SHUFFLE_PARALLELISM,
                        UPSERT_SHUFFLE_PARALLELISM,
                        MIN_COMMITS_TO_KEEP,
                        MAX_COMMITS_TO_KEEP,
                        CDC_ENABLED,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        HudiSinkConfig hudiSinkConfig = HudiSinkConfig.of(context.getOptions());
        CatalogTable catalogTable = context.getCatalogTable();
        HudiTableConfig hudiTableConfig =
                getHudiTableConfig(hudiSinkConfig, catalogTable.getTableId().getTableName());
        TableIdentifier tableId = catalogTable.getTableId();

        // rebuild TableIdentifier and catalogTable
        TableIdentifier newTableId =
                TableIdentifier.of(
                        tableId.getCatalogName(),
                        hudiTableConfig.getDatabase(),
                        tableId.getSchemaName(),
                        hudiTableConfig.getTableName());
        // partition keys
        List<String> finalPartitionKeys = catalogTable.getPartitionKeys();
        if (StringUtils.isNoneEmpty(hudiTableConfig.getPartitionFields())) {
            finalPartitionKeys = Arrays.asList(hudiTableConfig.getPartitionFields().split(","));
            catalogTable
                    .getOptions()
                    .put(PARTITION_FIELDS.key(), hudiTableConfig.getPartitionFields());
        }
        // record keys
        if (StringUtils.isNoneEmpty(hudiTableConfig.getRecordKeyFields())) {
            catalogTable
                    .getOptions()
                    .put(RECORD_KEY_FIELDS.key(), hudiTableConfig.getRecordKeyFields());
        }
        // table type
        catalogTable.getOptions().put(TABLE_TYPE.key(), hudiTableConfig.getTableType().name());
        // cdc enabled
        catalogTable
                .getOptions()
                .put(CDC_ENABLED.key(), String.valueOf(hudiTableConfig.isCdcEnabled()));
        catalogTable =
                CatalogTable.of(
                        newTableId,
                        catalogTable.getTableSchema(),
                        catalogTable.getOptions(),
                        finalPartitionKeys,
                        catalogTable.getComment(),
                        catalogTable.getCatalogName());
        // set record keys to options
        CatalogTable finalCatalogTable = catalogTable;
        return () ->
                new HudiSink(
                        context.getOptions(), hudiSinkConfig, hudiTableConfig, finalCatalogTable);
    }

    private HudiTableConfig getHudiTableConfig(HudiSinkConfig hudiSinkConfig, String tableName) {
        List<HudiTableConfig> tableList = hudiSinkConfig.getTableList();
        if (tableList.size() == 1) {
            return tableList.get(0);
        } else if (tableList.size() > 1) {
            Optional<HudiTableConfig> optionalHudiTableConfig =
                    tableList.stream()
                            .filter(table -> table.getTableName().equals(tableName))
                            .findFirst();
            if (!optionalHudiTableConfig.isPresent()) {
                throw new HudiConnectorException(
                        TABLE_CONFIG_NOT_FOUND,
                        "The corresponding table configuration is not found");
            }
            return optionalHudiTableConfig.get();
        }
        throw new HudiConnectorException(
                TABLE_CONFIG_NOT_FOUND, "The corresponding table configuration is not found");
    }
}
