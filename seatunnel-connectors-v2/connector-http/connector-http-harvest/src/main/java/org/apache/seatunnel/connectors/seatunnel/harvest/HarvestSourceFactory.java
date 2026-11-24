/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.harvest;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class HarvestSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Harvest";
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new HarvestSource(context.getOptions());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        HarvestSourceOptions.ACCESS_TOKEN,
                        HarvestSourceOptions.ACCOUNT_ID,
                        ConnectorCommonOptions.SCHEMA)
                .optional(
                        HarvestSourceOptions.BASE_URL,
                        HarvestSourceOptions.PAGE_SIZE,
                        HarvestSourceOptions.FROM,
                        HarvestSourceOptions.TO,
                        HarvestSourceOptions.UPDATED_SINCE,
                        HarvestSourceOptions.RETRY,
                        HarvestSourceOptions.RETRY_BACKOFF_MS,
                        HarvestSourceOptions.MAX_RETRY_DELAY_MS,
                        HarvestSourceOptions.CONNECT_TIMEOUT_MS,
                        HarvestSourceOptions.SOCKET_TIMEOUT_MS)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return HarvestSource.class;
    }
}
