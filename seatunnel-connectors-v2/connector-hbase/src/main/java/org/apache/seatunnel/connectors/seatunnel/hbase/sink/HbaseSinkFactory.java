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

package org.apache.seatunnel.connectors.seatunnel.hbase.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SinkReplaceNameConstant;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ENCODING;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.FAMILY_NAME;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.HBASE_EXTRA_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.NULL_MODE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ROWKEY_COLUMNS;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ROWKEY_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.VERSION_COLUMN;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.WAL_WRITE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.WRITE_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ZOOKEEPER_QUORUM;

@AutoService(Factory.class)
public class HbaseSinkFactory implements TableSinkFactory {

    public static final String IDENTIFIER = "Hbase";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(ZOOKEEPER_QUORUM, TABLE, ROWKEY_COLUMNS, FAMILY_NAME)
                .optional(
                        ROWKEY_DELIMITER,
                        VERSION_COLUMN,
                        NULL_MODE,
                        WAL_WRITE,
                        WRITE_BUFFER_SIZE,
                        ENCODING,
                        HBASE_EXTRA_CONFIG)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        ReadonlyConfig finalReadonlyConfig =
                generateCurrentReadonlyConfig(readonlyConfig, catalogTable);
        HbaseParameters hbaseParameters = HbaseParameters.buildWithConfig(finalReadonlyConfig);
        return () -> new HbaseSink(hbaseParameters, catalogTable);
    }

    private ReadonlyConfig generateCurrentReadonlyConfig(
            ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        Map<String, String> configMap = readonlyConfig.toMap();

        readonlyConfig
                .getOptional(TABLE)
                .ifPresent(
                        tableName -> {
                            String replacedPath =
                                    replaceCatalogTableInPath(tableName, catalogTable);
                            configMap.put(TABLE.key(), replacedPath);
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