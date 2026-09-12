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

package org.apache.seatunnel.connectors.seatunnel.facebook.ads.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.config.FacebookAdsParameters;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.config.FacebookAdsSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class FacebookAdsSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "FacebookAds";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        FacebookAdsSourceOptions.ACCESS_TOKEN,
                        FacebookAdsSourceOptions.AD_ACCOUNT_ID)
                .exclusive(FacebookAdsSourceOptions.RESOURCE, ConnectorCommonOptions.TABLE_CONFIGS)
                .optional(
                        FacebookAdsSourceOptions.API_VERSION,
                        FacebookAdsSourceOptions.FIELDS,
                        FacebookAdsSourceOptions.FILTERING,
                        FacebookAdsSourceOptions.PARAMS,
                        FacebookAdsSourceOptions.REQUEST_TIMEOUT_MS,
                        FacebookAdsSourceOptions.MAX_RETRIES,
                        FacebookAdsSourceOptions.RETRY_BACKOFF_MS,
                        FacebookAdsSourceOptions.PAGE_SIZE)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        FacebookAdsParameters params = new FacebookAdsParameters();
        params.buildWithConfig(context.getOptions());
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new FacebookAdsSource(params, context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return FacebookAdsSource.class;
    }
}
