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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.SupportMultipleTable;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions;

import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@AutoService(Factory.class)
public class MongodbIncrementalSourceFactory implements TableSourceFactory, SupportMultipleTable {
    @Override
    public String factoryIdentifier() {
        return MongodbIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return MongodbSourceOptions.getBaseRule()
                .required(
                        MongodbSourceOptions.HOSTS,
                        MongodbSourceOptions.DATABASE,
                        MongodbSourceOptions.COLLECTION)
                .optional(
                        MongodbSourceOptions.USERNAME,
                        MongodbSourceOptions.PASSWORD,
                        MongodbSourceOptions.CONNECTION_OPTIONS,
                        MongodbSourceOptions.BATCH_SIZE,
                        MongodbSourceOptions.POLL_MAX_BATCH_SIZE,
                        MongodbSourceOptions.POLL_AWAIT_TIME_MILLIS,
                        MongodbSourceOptions.HEARTBEAT_INTERVAL_MILLIS,
                        MongodbSourceOptions.INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB,
                        MongodbSourceOptions.STARTUP_MODE,
                        MongodbSourceOptions.STOP_MODE)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MongodbIncrementalSource.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableFactoryContext context) {
        return () -> {
            SeaTunnelDataType<SeaTunnelRow> dataType;
            if (context.getCatalogTables().size() == 1) {
                dataType =
                        context.getCatalogTables().get(0).getTableSchema().toPhysicalRowDataType();
            } else {
                Map<String, SeaTunnelRowType> rowTypeMap = new HashMap<>();
                for (CatalogTable catalogTable : context.getCatalogTables()) {
                    rowTypeMap.put(
                            catalogTable.getTableId().toTablePath().toString(),
                            catalogTable.getTableSchema().toPhysicalRowDataType());
                }
                dataType = new MultipleRowType(rowTypeMap);
            }
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new MongodbIncrementalSource<>(context.getOptions(), dataType);
        };
    }

    @Override
    public Result applyTables(@Nonnull TableFactoryContext context) {
        return Result.of(context.getCatalogTables(), Collections.emptyList());
    }
}
