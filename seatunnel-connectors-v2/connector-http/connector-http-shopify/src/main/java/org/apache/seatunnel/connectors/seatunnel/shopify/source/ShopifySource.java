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

package org.apache.seatunnel.connectors.seatunnel.shopify.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;
import org.apache.seatunnel.connectors.seatunnel.shopify.source.config.ShopifySourceParameter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
/**
 * Reads a Shopify Admin REST API resource — orders, products, customers — into SeaTunnel rows.
 *
 * <p>A thin wrapper over {@link HttpSource}: the only behaviour it adds is authentication, through
 * {@link ShopifySourceParameter}, which puts the configured token in {@code
 * X-Shopify-Access-Token}. Everything else — schema, format, retries — comes from the HTTP source
 * unchanged.
 */
public class ShopifySource extends HttpSource {
    private final ShopifySourceParameter shopifySourceParameter = new ShopifySourceParameter();

    public ShopifySource(ReadonlyConfig pluginConfig) {
        super(pluginConfig);
        rejectUnsupportedPagination(pluginConfig);
        this.shopifySourceParameter.buildWithConfig(pluginConfig);
    }

    /**
     * {@code pageing} is inherited from the HTTP source option rule, but this connector never hands
     * a {@code PageInfo} to its reader, so accepting the option would read the first response and
     * report success — a store with more than one page of results would lose the rest silently.
     * Failing at startup keeps that from looking like a healthy job.
     *
     * <p>The Admin REST API returns its cursor in the {@code Link} response header, while the
     * shared reader resolves cursors with a JsonPath against the response body, so supporting this
     * properly needs a header-cursor mode in {@code connector-http-base} rather than a change here.
     */
    private static void rejectUnsupportedPagination(ReadonlyConfig pluginConfig) {
        if (pluginConfig.getOptional(HttpSourceOptions.PAGEING).isPresent()) {
            throw new HttpConnectorException(
                    HttpConnectorErrorCode.CONFIG_VALIDATION_FAILED,
                    "The Shopify source does not support the 'pageing' option: it would be "
                            + "accepted and then ignored, so only the first page of results would "
                            + "be read while the job reported success. Remove 'pageing' from the "
                            + "source configuration.");
        }
    }

    @Override
    public String getPluginName() {
        return "Shopify";
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new HttpSourceReader(
                this.shopifySourceParameter,
                readerContext,
                this.deserializationSchema,
                jsonField,
                contentField);
    }
}
