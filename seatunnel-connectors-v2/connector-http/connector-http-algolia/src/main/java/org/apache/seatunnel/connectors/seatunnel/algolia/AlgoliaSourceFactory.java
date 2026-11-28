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
package org.apache.seatunnel.connectors.seatunnel.algolia;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

/** Discovers the Algolia table source through the SeaTunnel Factory SPI. */
@AutoService(Factory.class)
public final class AlgoliaSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AlgoliaSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        AlgoliaSourceOptions.APPLICATION_ID,
                        AlgoliaSourceOptions.TOKEN,
                        AlgoliaSourceOptions.START_DATE)
                .optional(
                        AlgoliaSourceOptions.BASE_URL,
                        AlgoliaSourceOptions.RESOURCES,
                        AlgoliaSourceOptions.SEARCH_QUERY,
                        AlgoliaSourceOptions.END_DATE,
                        AlgoliaSourceOptions.INDICES_PAGE_SIZE,
                        AlgoliaSourceOptions.LOGS_PAGE_SIZE,
                        AlgoliaSourceOptions.MAX_PAGES,
                        AlgoliaSourceOptions.MIN_INTERVAL_MS,
                        AlgoliaSourceOptions.MAX_ATTEMPTS,
                        AlgoliaSourceOptions.SOCKET_TIMEOUT_MS,
                        AlgoliaSourceOptions.PAGE_DEADLINE_MS,
                        AlgoliaSourceOptions.MAX_RESPONSE_BYTES)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new AlgoliaSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AlgoliaSource.class;
    }
}
