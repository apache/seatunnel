/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.sls.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.sls.config.SlsSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class SlsSourceFactory implements TableSourceFactory {
    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return (Class<? extends SeaTunnelSource>) SlsSource.class;
    }

    @Override
    public String factoryIdentifier() {
        return SlsSourceOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        SlsSourceOptions.ENDPOINT,
                        SlsSourceOptions.PROJECT,
                        SlsSourceOptions.LOGSTORE,
                        SlsSourceOptions.ACCESS_KEY_ID,
                        SlsSourceOptions.ACCESS_KEY_SECRET)
                .optional(
                        SlsSourceOptions.BATCH_SIZE,
                        SlsSourceOptions.START_MODE,
                        SlsSourceOptions.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
                        SlsSourceOptions.AUTO_CURSOR_RESET,
                        SlsSourceOptions.CONSUMER_GROUP)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new SlsSource(context.getOptions());
    }
}
