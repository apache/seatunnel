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

package org.apache.seatunnel.connectors.seatunnel.appsflyer;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

/**
 * Registers the report source and validates the selection before allocating any network resources.
 * The standard SeaTunnel source SPI owns all reporting window and checkpoint transitions.
 */
@AutoService(Factory.class)
public final class AppsFlyerSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AppsFlyerSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(AppsFlyerConfig.TOKEN, AppsFlyerConfig.APP_ID, AppsFlyerConfig.START_DATE)
                .optional(
                        AppsFlyerConfig.END_DATE,
                        AppsFlyerConfig.TIMEZONE,
                        AppsFlyerConfig.RESOURCES,
                        AppsFlyerConfig.API_URL,
                        AppsFlyerConfig.DOWNLOAD_ORIGINS,
                        AppsFlyerConfig.MAX_WINDOW_RECORDS,
                        AppsFlyerConfig.MAX_RESPONSE_BYTES,
                        AppsFlyerConfig.MAX_BUFFER_BYTES,
                        AppsFlyerConfig.MAX_FIELD_CHARS,
                        AppsFlyerConfig.MAX_REQUESTS,
                        AppsFlyerConfig.CONNECT_TIMEOUT,
                        AppsFlyerConfig.READ_TIMEOUT,
                        AppsFlyerConfig.REQUEST_TIMEOUT,
                        AppsFlyerConfig.MAX_ATTEMPTS)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        AppsFlyerSource source = new AppsFlyerSource(new AppsFlyerConfig(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AppsFlyerSource.class;
    }
}
