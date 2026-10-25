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

package org.apache.seatunnel.connectors.seatunnel.google.analytics4;

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
public class GoogleAnalytics4SourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return GoogleAnalytics4Source.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        GoogleAnalytics4SourceOptions.PROPERTY_ID,
                        GoogleAnalytics4SourceOptions.START_DATE,
                        GoogleAnalytics4SourceOptions.END_DATE,
                        GoogleAnalytics4SourceOptions.METRICS,
                        GoogleAnalytics4SourceOptions.METRIC_TYPES,
                        ConnectorCommonOptions.SCHEMA)
                .exclusive(
                        GoogleAnalytics4SourceOptions.KEY_FILE,
                        GoogleAnalytics4SourceOptions.EMULATOR_URL)
                .optional(
                        GoogleAnalytics4SourceOptions.DIMENSIONS,
                        GoogleAnalytics4SourceOptions.PAGE_SIZE,
                        GoogleAnalytics4SourceOptions.MAX_ROWS,
                        GoogleAnalytics4SourceOptions.MAX_BYTES,
                        GoogleAnalytics4SourceOptions.TIMEOUT,
                        GoogleAnalytics4SourceOptions.REPORT_TIMEOUT,
                        GoogleAnalytics4SourceOptions.RETRIES,
                        GoogleAnalytics4SourceOptions.BACKOFF)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        GoogleAnalytics4Source source = new GoogleAnalytics4Source(context.getOptions());
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return GoogleAnalytics4Source.class;
    }
}
