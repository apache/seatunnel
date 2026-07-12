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

package org.apache.seatunnel.connectors.seatunnel.hbase.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.hbase.constant.HbaseIdentifier;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class HbaseSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return HbaseIdentifier.IDENTIFIER_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HbaseSourceOptions.ZOOKEEPER_QUORUM)
                .required(HbaseSourceOptions.TABLE)
                .optional(
                        HbaseBaseOptions.HBASE_EXTRA_CONFIG,
                        HbaseSourceOptions.HBASE_CACHING_CONFIG,
                        HbaseSourceOptions.HBASE_BATCH_CONFIG,
                        HbaseSourceOptions.HBASE_CACHE_BLOCKS_CONFIG,
                        HbaseSourceOptions.IS_BINARY_ROW_KEY,
                        HbaseSourceOptions.START_ROW_KEY,
                        HbaseSourceOptions.END_ROW_KEY,
                        HbaseSourceOptions.START_ROW_INCLUSIVE,
                        HbaseSourceOptions.END_ROW_INCLUSIVE,
                        HbaseSourceOptions.START_TIMESTAMP,
                        HbaseSourceOptions.END_TIMESTAMP)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return HbaseSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new HbaseSource(
                                HbaseParameters.buildWithSourceConfig(context.getOptions()),
                                CatalogTableUtil.buildWithConfig(context.getOptions()));
    }
}
