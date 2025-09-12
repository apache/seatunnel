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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class MaxcomputeSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return MaxcomputeSourceOptions.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        MaxcomputeSourceOptions.ACCESS_ID,
                        MaxcomputeSourceOptions.ACCESS_KEY,
                        MaxcomputeSourceOptions.ENDPOINT)
                .optional(
                        MaxcomputeSourceOptions.PARTITION_SPEC,
                        MaxcomputeSourceOptions.SPLIT_ROW,
                        ConnectorCommonOptions.SCHEMA,
                        MaxcomputeSourceOptions.PROJECT,
                        MaxcomputeSourceOptions.READ_COLUMNS,
                        MaxcomputeSourceOptions.TUNNEL_ENDPOINT)
                .exclusive(MaxcomputeSourceOptions.TABLE_LIST, MaxcomputeSourceOptions.TABLE_NAME)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MaxcomputeSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>) new MaxcomputeSource(context.getOptions());
    }
}
