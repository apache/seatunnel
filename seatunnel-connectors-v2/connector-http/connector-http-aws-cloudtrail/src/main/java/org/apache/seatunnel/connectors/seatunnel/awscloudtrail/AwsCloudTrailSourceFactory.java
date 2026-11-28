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

package org.apache.seatunnel.connectors.seatunnel.awscloudtrail;

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
 * Registers the AWS CloudTrail management event source through the normal table factory SPI.
 * Configuration is checked before any steady-state network activity is possible.
 */
@AutoService(Factory.class)
public final class AwsCloudTrailSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AwsCloudTrailConfig.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(AwsCloudTrailConfig.ACCESS_KEY, AwsCloudTrailConfig.SECRET_KEY)
                .optional(
                        AwsCloudTrailConfig.REGION,
                        AwsCloudTrailConfig.SECURITY_TOKEN,
                        AwsCloudTrailConfig.START_DATE,
                        AwsCloudTrailConfig.END_DATE,
                        AwsCloudTrailConfig.FILTER_KEY,
                        AwsCloudTrailConfig.FILTER_VALUE,
                        AwsCloudTrailConfig.PAGE_SIZE,
                        AwsCloudTrailConfig.MAX_PAGES,
                        AwsCloudTrailConfig.MAX_RESPONSE_BYTES,
                        AwsCloudTrailConfig.CONNECT_TIMEOUT,
                        AwsCloudTrailConfig.READ_TIMEOUT,
                        AwsCloudTrailConfig.REQUEST_TIMEOUT,
                        AwsCloudTrailConfig.MAX_ATTEMPTS)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        AwsCloudTrailSource source =
                new AwsCloudTrailSource(new AwsCloudTrailConfig(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AwsCloudTrailSource.class;
    }
}
