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

package org.apache.seatunnel.connectors.seatunnel.google.ads.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.google.ads.config.GoogleAdsParameters;
import org.apache.seatunnel.connectors.seatunnel.google.ads.config.GoogleAdsSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class GoogleAdsSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "GoogleAds";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        GoogleAdsSourceOptions.DEVELOPER_TOKEN,
                        GoogleAdsSourceOptions.CLIENT_ID,
                        GoogleAdsSourceOptions.CLIENT_SECRET,
                        GoogleAdsSourceOptions.REFRESH_TOKEN,
                        GoogleAdsSourceOptions.CUSTOMER_ID)
                .exclusive(
                        GoogleAdsSourceOptions.RESOURCE,
                        GoogleAdsSourceOptions.QUERY,
                        ConnectorCommonOptions.TABLE_CONFIGS)
                .optional(
                        GoogleAdsSourceOptions.LOGIN_CUSTOMER_ID,
                        GoogleAdsSourceOptions.API_VERSION,
                        GoogleAdsSourceOptions.FIELDS,
                        GoogleAdsSourceOptions.FILTER,
                        GoogleAdsSourceOptions.REQUEST_TIMEOUT_MS,
                        GoogleAdsSourceOptions.MAX_RETRIES,
                        GoogleAdsSourceOptions.RETRY_BACKOFF_MS,
                        GoogleAdsSourceOptions.PAGE_SIZE)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        GoogleAdsParameters params = new GoogleAdsParameters();
        params.buildWithConfig(context.getOptions());
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new GoogleAdsSource(params, context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return GoogleAdsSource.class;
    }
}
