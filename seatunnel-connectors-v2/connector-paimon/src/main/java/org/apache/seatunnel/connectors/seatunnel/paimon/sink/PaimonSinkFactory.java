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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.WAREHOUSE;

@AutoService(Factory.class)
public class PaimonSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "Paimon";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(PaimonConfig.WAREHOUSE)
                .required(PaimonConfig.DATABASE)
                .required(PaimonConfig.TABLE)
                .optional(PaimonConfig.HDFS_SITE_PATH)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        Config pluginConfig = context.getOptions().toConfig();
        CatalogTable catalogTable = renameCatalogTable(pluginConfig, context.getCatalogTable());
        return () -> new PaimonSink(pluginConfig, catalogTable);
    }

    private CatalogTable renameCatalogTable(Config pluginConfig, CatalogTable catalogTable) {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig, WAREHOUSE.key(), DATABASE.key(), TABLE.key());
        if (!result.isSuccess()) {
            throw new PaimonConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            factoryIdentifier(), PluginType.SINK, result.getMsg()));
        }
        TableIdentifier tableId = catalogTable.getTableId();
        String tableName = pluginConfig.getString(TABLE.key());
        String namespace = pluginConfig.getString(DATABASE.key());

        TableIdentifier newTableId =
                TableIdentifier.of(
                        tableId.getCatalogName(), namespace, tableId.getSchemaName(), tableName);

        return CatalogTable.of(newTableId, catalogTable);
    }
}
