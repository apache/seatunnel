/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hubspot.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@AutoService(Factory.class)
public class HubSpotSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "HubSpot";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HubSpotSourceOptions.ACCESS_TOKEN)
                .optional(HubSpotSourceOptions.OBJECT_TYPE)
                .optional(HttpCommonOptions.URL)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return HubSpotSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        Map<String, Object> configMap = new HashMap<>(options.toMap());

        // Reviewer Fix: Safely inject the default content_field if user didn't provide one
        if (!configMap.containsKey("content_field")) {
            configMap.put("content_field", HubSpotSourceParameter.DEFAULT_CONTENT_FIELD);
        }

        ReadonlyConfig modifiedConfig = ReadonlyConfig.fromMap(configMap);
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new HubSpotSource(modifiedConfig);
    }
}
