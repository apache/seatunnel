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
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

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
                .required(HudiSinkOptions.TABLE_DFS_PATH)
                .exclusive(HudiSinkOptions.TABLE_NAME, HudiSinkOptions.TABLE_LIST)
                .optional(
                        HudiSinkOptions.TABLE_TYPE,
                        HudiSinkOptions.RECORD_KEY_FIELDS,
                        HudiSinkOptions.PARTITION_FIELDS,
                        HudiSinkOptions.INDEX_TYPE,
                        HudiSinkOptions.INDEX_CLASS_NAME,
                        HudiSinkOptions.RECORD_BYTE_SIZE,
                        HudiSinkOptions.CONF_FILES_PATH,
                        HudiSinkOptions.OP_TYPE,
                        HudiSinkOptions.BATCH_SIZE,
                        HudiSinkOptions.BATCH_INTERVAL_MS,
                        HudiSinkOptions.INSERT_SHUFFLE_PARALLELISM,
                        HudiSinkOptions.UPSERT_SHUFFLE_PARALLELISM,
                        HudiSinkOptions.MIN_COMMITS_TO_KEEP,
                        HudiSinkOptions.MAX_COMMITS_TO_KEEP,
                        HudiSinkOptions.CDC_ENABLED,
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
                    .put(
                            HudiSinkOptions.PARTITION_FIELDS.key(),
                            hudiTableConfig.getPartitionFields());
        }
        // record keys
        if (StringUtils.isNoneEmpty(hudiTableConfig.getRecordKeyFields())) {
            catalogTable
                    .getOptions()
                    .put(
                            HudiSinkOptions.RECORD_KEY_FIELDS.key(),
                            hudiTableConfig.getRecordKeyFields());
        }
        // table type
        catalogTable
                .getOptions()
                .put(HudiSinkOptions.TABLE_TYPE.key(), hudiTableConfig.getTableType().name());
        // cdc enabled
        catalogTable
                .getOptions()
                .put(
                        HudiSinkOptions.CDC_ENABLED.key(),
                        String.valueOf(hudiTableConfig.isCdcEnabled()));
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
