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
package org.apache.seatunnel.connectors.seatunnel.amazonads;

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
 * Registers the profile-scoped Amazon Advertising batch source through the Factory SPI.
 *
 * <p>Option validation occurs before the source opens a provider connection.
 */
@AutoService(Factory.class)
public final class AmazonAdsSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AmazonAdsSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        AmazonAdsSourceOptions.CLIENT_ID,
                        AmazonAdsSourceOptions.CLIENT_SECRET,
                        AmazonAdsSourceOptions.REFRESH_TOKEN)
                .optional(
                        AmazonAdsSourceOptions.REGION,
                        AmazonAdsSourceOptions.BASE_URL,
                        AmazonAdsSourceOptions.OAUTH_BASE_URL,
                        AmazonAdsSourceOptions.DOWNLOAD_HOSTS,
                        AmazonAdsSourceOptions.RESOURCES,
                        AmazonAdsSourceOptions.PROFILES,
                        AmazonAdsSourceOptions.MARKETPLACE_IDS,
                        AmazonAdsSourceOptions.STATE_FILTER,
                        AmazonAdsSourceOptions.START_DATE,
                        AmazonAdsSourceOptions.END_DATE,
                        AmazonAdsSourceOptions.LOOK_BACK_WINDOW,
                        AmazonAdsSourceOptions.REPORT_WINDOW_DAYS,
                        AmazonAdsSourceOptions.PAGE_SIZE,
                        AmazonAdsSourceOptions.MAX_PAGES,
                        AmazonAdsSourceOptions.MAX_PROFILES,
                        AmazonAdsSourceOptions.MAX_ATTEMPTS,
                        AmazonAdsSourceOptions.REQUEST_INTERVAL_MS,
                        AmazonAdsSourceOptions.IO_TIMEOUT_MS,
                        AmazonAdsSourceOptions.REQUEST_DEADLINE_MS,
                        AmazonAdsSourceOptions.POLL_INTERVAL_MS,
                        AmazonAdsSourceOptions.REPORT_TIMEOUT_MS,
                        AmazonAdsSourceOptions.DUPLICATE_WAIT_MS,
                        AmazonAdsSourceOptions.DOWNLOAD_DEADLINE_MS,
                        AmazonAdsSourceOptions.MAX_RESPONSE_BYTES,
                        AmazonAdsSourceOptions.MAX_DOWNLOAD_BYTES,
                        AmazonAdsSourceOptions.MAX_REPORT_BYTES,
                        AmazonAdsSourceOptions.MAX_REPORT_ROWS)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new AmazonAdsSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AmazonAdsSource.class;
    }
}
