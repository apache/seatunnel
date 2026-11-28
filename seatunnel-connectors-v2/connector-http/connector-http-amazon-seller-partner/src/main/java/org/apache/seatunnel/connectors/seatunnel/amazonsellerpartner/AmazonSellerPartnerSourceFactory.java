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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.ACCOUNT_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.ASIN_GRANULARITY;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.CLIENT_ID;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.CLIENT_SECRET;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.END_TIME;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.ENVIRONMENT;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.FINANCIAL_EVENTS_STEP;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.FINANCIAL_PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.INBOUND_END_DATETIME;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.INBOUND_REPLICATION_MODE;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.INBOUND_ROLLING_DAYS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.INBOUND_START_DATETIME;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.INCLUDE_PII;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.MAX_DONE_REPORT_AGE_HOURS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.MAX_RATE_LIMIT_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.MAX_REPORT_ROWS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.MAX_REQUESTS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.PERIOD_IN_DAYS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.POLL_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.REGION;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.REPORT_LOOKBACK_HOURS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.REPORT_OPTIONS_LIST;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.REPORT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.SANDBOX_ORIGIN;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.START_TIME;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.STOP_SYNC_ON_RATE_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.TREAT_CANCELLED_AS_EMPTY;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.TRUST_CERTIFICATE;

/**
 * Registers the Amazon Selling Partner API source with the Factory SPI.
 *
 * <p>Resource, marketplace and credential options are validated before work is assigned.
 */
@AutoService(Factory.class)
public final class AmazonSellerPartnerSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AmazonSellerPartnerSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(RESOURCE, CLIENT_ID, CLIENT_SECRET, TOKEN)
                .optional(
                        ACCOUNT_TYPE,
                        ENVIRONMENT,
                        REGION,
                        INCLUDE_PII,
                        START_TIME,
                        END_TIME,
                        PERIOD_IN_DAYS,
                        FINANCIAL_EVENTS_STEP,
                        FINANCIAL_PAGE_SIZE,
                        INBOUND_REPLICATION_MODE,
                        INBOUND_ROLLING_DAYS,
                        INBOUND_START_DATETIME,
                        INBOUND_END_DATETIME,
                        REPORT_OPTIONS_LIST,
                        ASIN_GRANULARITY,
                        REPORT_LOOKBACK_HOURS,
                        MAX_DONE_REPORT_AGE_HOURS,
                        TREAT_CANCELLED_AS_EMPTY,
                        MAX_REQUESTS,
                        MAX_ATTEMPTS,
                        MAX_RATE_LIMIT_RETRIES,
                        STOP_SYNC_ON_RATE_LIMIT,
                        REQUEST_TIMEOUT_MS,
                        REPORT_TIMEOUT_MS,
                        POLL_INTERVAL_MS,
                        MAX_RESPONSE_BYTES,
                        MAX_REPORT_ROWS,
                        SANDBOX_ORIGIN,
                        TRUST_CERTIFICATE)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new AmazonSellerPartnerSource(new SpSettings(context.getOptions()));
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AmazonSellerPartnerSource.class;
    }
}
