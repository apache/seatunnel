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

package org.apache.seatunnel.connectors.seatunnel.appfollow;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

/**
 * Registers the AppFollow table source through the normal SeaTunnel factory SPI. The selected
 * resource graph, credentials and limits are validated before network access.
 */
@AutoService(Factory.class)
public final class AppfollowSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AppfollowSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(AppfollowConfig.TOKEN)
                .optional(
                        AppfollowConfig.RESOURCES,
                        AppfollowConfig.API_URL,
                        AppfollowConfig.MAX_ENTITIES,
                        AppfollowConfig.MAX_REQUESTS,
                        AppfollowConfig.MAX_RECORDS,
                        AppfollowConfig.MAX_RESPONSE,
                        AppfollowConfig.CONNECT_TIMEOUT,
                        AppfollowConfig.READ_TIMEOUT,
                        AppfollowConfig.REQUEST_TIMEOUT,
                        AppfollowConfig.MAX_ATTEMPTS)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        AppfollowSource source = new AppfollowSource(new AppfollowConfig(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AppfollowSource.class;
    }
}
