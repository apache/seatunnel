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

package org.apache.seatunnel.connectors.seatunnel.paimon.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig;

import org.apache.paimon.table.Table;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.WAREHOUSE;

@AutoService(Factory.class)
public class PaimonSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "Paimon";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(WAREHOUSE)
                .required(PaimonConfig.DATABASE)
                .required(PaimonConfig.TABLE)
                .optional(PaimonConfig.HDFS_SITE_PATH)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return PaimonSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        PaimonConfig sourceConfig = new PaimonConfig(config);
        Optional<Catalog> optionalCatalog =
                FactoryUtil.createOptionalCatalog(
                        sourceConfig.getCatalogName(),
                        config,
                        sourceConfig.getClass().getClassLoader(),
                        PaimonCatalogFactory.IDENTIFIER);
        PaimonCatalog paimonCatalog = (PaimonCatalog) optionalCatalog.get();
        TablePath tablePath = TablePath.of(sourceConfig.getNamespace(), sourceConfig.getTable());
        Table paimonTable = paimonCatalog.getPaimonTable(tablePath);
        CatalogTable catalogTable = paimonCatalog.getTable(tablePath);

        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new PaimonSource(sourceConfig, catalogTable, paimonTable);
    }
}
