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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;

/** HubSpot source backed by the shared HTTP source implementation. */
public class HubSpotSource extends HttpSource {

    /** Keeps the HubSpot-specific URL, auth header, and paging defaults for the reader path. */
    private final HubSpotSourceParameter hubSpotSourceParameter = new HubSpotSourceParameter();

    public HubSpotSource(ReadonlyConfig config) {
        super(HubSpotSourceParameter.buildRuntimeConfig(config));
        hubSpotSourceParameter.buildWithConfig(config);

        if (!config.getOptional(HttpSourceOptions.CONTENT_FIELD).isPresent()) {
            this.contentField = HubSpotSourceParameter.DEFAULT_CONTENT_FIELD;
        }
    }

    @Override
    public String getPluginName() {
        return "HubSpot";
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        // Reuse the HubSpot-specific parameter object so the reader sees the injected auth, URL,
        // and cursor pagination defaults instead of rebuilding a generic HTTP parameter.
        return new HttpSourceReader(
                this.hubSpotSourceParameter,
                readerContext,
                this.deserializationSchema,
                jsonField,
                contentField,
                pageInfo,
                binaryMode,
                binaryChunkSize);
    }
}
