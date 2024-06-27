/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.druid.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SinkReplaceNameConstant;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.druid.config.DruidConfig.COORDINATOR_URL;
import static org.apache.seatunnel.connectors.druid.config.DruidConfig.DATASOURCE;

@AutoService(Factory.class)
public class DruidSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Druid";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(COORDINATOR_URL, DATASOURCE).build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();

        ReadonlyConfig finalReadonlyConfig =
                generateCurrentReadonlyConfig(readonlyConfig, catalogTable);
        return () -> new DruidSink(finalReadonlyConfig, catalogTable);
    }

    private ReadonlyConfig generateCurrentReadonlyConfig(
            ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {

        Map<String, String> configMap = readonlyConfig.toMap();

        readonlyConfig
                .getOptional(DATASOURCE)
                .ifPresent(
                        tableName -> {
                            String replacedPath =
                                    replaceCatalogTableInPath(tableName, catalogTable);
                            configMap.put(DATASOURCE.key(), replacedPath);
                        });

        return ReadonlyConfig.fromMap(new HashMap<>(configMap));
    }

    private String replaceCatalogTableInPath(String originTableName, CatalogTable catalogTable) {
        String tableName = originTableName;
        TableIdentifier tableIdentifier = catalogTable.getTableId();
        if (tableIdentifier != null) {
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
