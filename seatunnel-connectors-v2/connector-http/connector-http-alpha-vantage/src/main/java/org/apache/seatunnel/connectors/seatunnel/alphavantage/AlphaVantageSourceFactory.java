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

package org.apache.seatunnel.connectors.seatunnel.alphavantage;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

/**
 * Discovers the source for the plugin identifier and validates the requested provider functions
 * before the engine creates or assigns its reader.
 */
@AutoService(Factory.class)
public final class AlphaVantageSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AlphaVantageSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(AlphaVantageOptions.TOKEN, AlphaVantageOptions.SYMBOL)
                .optional(
                        AlphaVantageOptions.API_URL,
                        AlphaVantageOptions.RESOURCES,
                        AlphaVantageOptions.INTERVAL,
                        AlphaVantageOptions.ADJUSTED,
                        AlphaVantageOptions.OUTPUT_SIZE,
                        AlphaVantageOptions.MAX_RECORDS,
                        AlphaVantageOptions.MAX_RESPONSE,
                        AlphaVantageOptions.CONNECT_TIMEOUT,
                        AlphaVantageOptions.READ_TIMEOUT,
                        AlphaVantageOptions.REQUEST_TIMEOUT,
                        AlphaVantageOptions.REQUEST_INTERVAL,
                        AlphaVantageOptions.MAX_ATTEMPTS,
                        AlphaVantageOptions.MAX_REQUESTS)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        AlphaVantageSource source =
                new AlphaVantageSource(new AlphaVantageConfig(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AlphaVantageSource.class;
    }
}
