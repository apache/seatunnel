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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
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
import org.apache.seatunnel.connectors.doris.config.DorisConfig;
import org.apache.seatunnel.connectors.doris.config.DorisOptions;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@AutoService(Factory.class)
public class DorisSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return DorisConfig.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DorisOptions.FENODES,
                        DorisOptions.USERNAME,
                        DorisOptions.PASSWORD,
                        DorisOptions.DATABASE,
                        DorisOptions.TABLE)
                .optional(DorisOptions.DORIS_FILTER_QUERY)
                .optional(DorisOptions.DORIS_READ_FIELD)
                .optional(DorisOptions.QUERY_PORT)
                .optional(DorisOptions.DORIS_BATCH_SIZE)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        CatalogTable table;
        DorisCatalogFactory dorisCatalogFactory = new DorisCatalogFactory();
        DorisCatalog catalog = (DorisCatalog) dorisCatalogFactory.createCatalog("doris", options);
        catalog.open();
        String tableIdentifier =
                options.get(DorisOptions.DATABASE) + "." + options.get(DorisOptions.TABLE);
        TablePath tablePath = TablePath.of(tableIdentifier);

        try {
            String read_fields = options.get(DorisOptions.DORIS_READ_FIELD);
            List<String> readFiledList = null;
            if (StringUtils.isNotBlank(read_fields)) {
                readFiledList =
                        Arrays.stream(read_fields.split(","))
                                .map(String::trim)
                                .collect(Collectors.toList());
            }

            table = catalog.getTable(tablePath, readFiledList);
        } catch (Exception e) {
            log.error("create source error");
            throw e;
        }
        CatalogTable finalTable = table;
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new DorisSource(options, finalTable);
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return DorisSource.class;
    }
}
