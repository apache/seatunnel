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

package org.apache.seatunnel.connectors.doris.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalog;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalogFactory;
import org.apache.seatunnel.connectors.doris.config.DorisSourceConfig;
import org.apache.seatunnel.connectors.doris.config.DorisSourceOptions;
import org.apache.seatunnel.connectors.doris.config.DorisTableConfig;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@AutoService(Factory.class)
public class DorisSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return DorisSourceOptions.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DorisSourceOptions.FENODES,
                        DorisSourceOptions.USERNAME,
                        DorisSourceOptions.PASSWORD)
                .optional(DorisSourceOptions.TABLE_LIST)
                .optional(DorisSourceOptions.DATABASE)
                .optional(DorisSourceOptions.TABLE)
                .optional(DorisSourceOptions.DORIS_FILTER_QUERY)
                .optional(DorisSourceOptions.DORIS_TABLET_SIZE)
                .optional(DorisSourceOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS)
                .optional(DorisSourceOptions.DORIS_REQUEST_READ_TIMEOUT_MS)
                .optional(DorisSourceOptions.DORIS_REQUEST_QUERY_TIMEOUT_S)
                .optional(DorisSourceOptions.DORIS_REQUEST_RETRIES)
                .optional(DorisSourceOptions.DORIS_DESERIALIZE_ARROW_ASYNC)
                .optional(DorisSourceOptions.DORIS_DESERIALIZE_QUEUE_SIZE)
                .optional(DorisSourceOptions.DORIS_READ_FIELD)
                .optional(DorisSourceOptions.QUERY_PORT)
                .optional(DorisSourceOptions.DORIS_BATCH_SIZE)
                .optional(DorisSourceOptions.DORIS_EXEC_MEM_LIMIT)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        DorisSourceConfig dorisSourceConfig = DorisSourceConfig.of(context.getOptions());
        List<DorisTableConfig> dorisTableConfigList = dorisSourceConfig.getTableConfigList();
        Map<TablePath, DorisSourceTable> dorisSourceTables = new HashMap<>();

        DorisCatalogFactory dorisCatalogFactory = new DorisCatalogFactory();
        try (DorisCatalog catalog =
                (DorisCatalog) dorisCatalogFactory.createCatalog("doris", context.getOptions())) {
            catalog.open();
            for (DorisTableConfig dorisTableConfig : dorisTableConfigList) {
                CatalogTable table;
                TablePath tablePath = TablePath.of(dorisTableConfig.getTableIdentifier());
                String readFields = dorisTableConfig.getReadField();
                try {
                    List<String> readFiledList = null;
                    if (StringUtils.isNotBlank(readFields)) {
                        readFiledList =
                                Arrays.stream(readFields.split(","))
                                        .map(String::trim)
                                        .collect(Collectors.toList());
                    }

                    table = catalog.getTable(tablePath, readFiledList);
                } catch (Exception e) {
                    log.error("create source error");
                    throw e;
                }
                dorisSourceTables.put(
                        tablePath,
                        DorisSourceTable.builder()
                                .catalogTable(table)
                                .tablePath(tablePath)
                                .readField(readFields)
                                .filterQuery(dorisTableConfig.getFilterQuery())
                                .batchSize(dorisTableConfig.getBatchSize())
                                .tabletSize(dorisTableConfig.getTabletSize())
                                .execMemLimit(dorisTableConfig.getExecMemLimit())
                                .build());
            }
        }
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new DorisSource(dorisSourceConfig, dorisSourceTables);
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return DorisSource.class;
    }
}
