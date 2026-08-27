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

package org.apache.seatunnel.connectors.seatunnel.airtable.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.airtable.source.config.AirtableSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.airtable.source.config.AirtableSourceParameter;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpPaginationType;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;

public class AirtableSource extends HttpSource {

    public static final String PLUGIN_NAME = "Airtable";

    private final AirtableSourceParameter airtableSourceParameter = new AirtableSourceParameter();
    private final int requestIntervalMs;
    private final int rateLimitBackoffMs;
    private final int rateLimitMaxRetries;

    public AirtableSource(ReadonlyConfig pluginConfig) {
        super(pluginConfig);
        airtableSourceParameter.buildWithConfig(pluginConfig);
        this.requestIntervalMs = pluginConfig.get(AirtableSourceOptions.REQUEST_INTERVAL_MS);
        this.rateLimitBackoffMs = pluginConfig.get(AirtableSourceOptions.RATE_LIMIT_BACKOFF_MS);
        this.rateLimitMaxRetries = pluginConfig.get(AirtableSourceOptions.RATE_LIMIT_MAX_RETRIES);
        if (this.pageInfo == null) {
            PageInfo info = new PageInfo();
            info.setPageType(HttpPaginationType.CURSOR.getCode());
            info.setPageCursorFieldName("offset");
            info.setPageCursorResponseField("$.offset");
            info.setUsePlaceholderReplacement(false);
            // Avoid NPE in HttpSourceReader.updateRequestParam for cursor pagination
            // (pageIndex is unused for cursor mode but referenced defensively).
            info.setPageIndex(0L);
            this.pageInfo = info;
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public Boundedness getBoundedness() {
        if (JobMode.BATCH.equals(jobContext.getJobMode())) {
            return Boundedness.BOUNDED;
        }
        throw new UnsupportedOperationException(
                "Airtable source connector not support unbounded operation");
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new AirtableSourceReader(
                airtableSourceParameter,
                readerContext,
                deserializationSchema,
                jsonField,
                contentField,
                pageInfo,
                requestIntervalMs,
                rateLimitBackoffMs,
                rateLimitMaxRetries);
    }
}
