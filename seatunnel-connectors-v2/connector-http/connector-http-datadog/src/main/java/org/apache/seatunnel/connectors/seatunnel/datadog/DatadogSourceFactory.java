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

package org.apache.seatunnel.connectors.seatunnel.datadog;

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
 * Registers the independently packaged Datadog source in SeaTunnel's Factory SPI. Generated
 * discovery binds the product identifier to its dedicated artifact rather than Http.
 */
@AutoService(Factory.class)
public class DatadogSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return DatadogSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DatadogSourceOptions.RESOURCE,
                        DatadogSourceOptions.ACCESS_KEY,
                        DatadogSourceOptions.SECRET_KEY)
                .optional(
                        DatadogSourceOptions.SITE,
                        DatadogSourceOptions.START_TIME,
                        DatadogSourceOptions.END_TIME,
                        DatadogSourceOptions.LOGS_QUERY,
                        DatadogSourceOptions.SLICE_DAYS,
                        DatadogSourceOptions.METRICS_WINDOW_SECONDS,
                        DatadogSourceOptions.PAGE_SIZE,
                        DatadogSourceOptions.REQUEST_TIMEOUT_MS,
                        DatadogSourceOptions.PAGE_DEADLINE_MS,
                        DatadogSourceOptions.MAX_ATTEMPTS,
                        DatadogSourceOptions.MAX_RESPONSE_BYTES)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new DatadogSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return DatadogSource.class;
    }
}
