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

package org.apache.seatunnel.connectors.seatunnel.apify;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

/**
 * Exposes selected Apify resources through the SeaTunnel table source SPI. The factory validates
 * configuration without opening a network connection.
 */
@AutoService(Factory.class)
public final class ApifyDatasetSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return ApifyDatasetSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(ApifyDatasetConfig.TOKEN)
                .optional(
                        ApifyDatasetConfig.DATASET_ID,
                        ApifyDatasetConfig.RESOURCES,
                        ApifyDatasetConfig.API_URL,
                        ApifyDatasetConfig.PAGE_SIZE,
                        ApifyDatasetConfig.MAX_PAGES,
                        ApifyDatasetConfig.MAX_RESPONSE,
                        ApifyDatasetConfig.CONNECT_TIMEOUT,
                        ApifyDatasetConfig.READ_TIMEOUT,
                        ApifyDatasetConfig.REQUEST_TIMEOUT,
                        ApifyDatasetConfig.MAX_ATTEMPTS)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ApifyDatasetSource source =
                new ApifyDatasetSource(new ApifyDatasetConfig(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return ApifyDatasetSource.class;
    }
}
