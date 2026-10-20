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

package org.apache.seatunnel.connectors.seatunnel.bigtable.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.bigtable.constant.BigtableIdentifier;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class BigtableSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return BigtableIdentifier.IDENTIFIER_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        BigtableBaseOptions.PROJECT_ID,
                        BigtableBaseOptions.INSTANCE_ID,
                        BigtableBaseOptions.TABLE)
                .optional(
                        BigtableBaseOptions.CREDENTIALS_PATH,
                        BigtableBaseOptions.ROWKEY_COLUMNS,
                        BigtableSourceOptions.START_ROW_KEY,
                        BigtableSourceOptions.END_ROW_KEY,
                        BigtableSourceOptions.START_TIMESTAMP,
                        BigtableSourceOptions.END_TIMESTAMP,
                        BigtableSourceOptions.MAX_VERSIONS,
                        BigtableSourceOptions.SCAN_ROW_LIMIT)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return BigtableSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new BigtableSource(
                                BigtableParameters.buildWithSourceConfig(context.getOptions()),
                                CatalogTableUtil.buildWithConfig(context.getOptions()));
    }
}
