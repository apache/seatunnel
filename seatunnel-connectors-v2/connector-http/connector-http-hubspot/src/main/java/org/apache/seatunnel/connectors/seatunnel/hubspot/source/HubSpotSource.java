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
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HubSpotSource extends HttpSource {

    private final HubSpotSourceParameter hubSpotSourceParameter = new HubSpotSourceParameter();

    public HubSpotSource(ReadonlyConfig config) {
        super(config);
        // Build the custom parameter (injects tokens, builds URL)
        hubSpotSourceParameter.buildWithConfig(config);

        // Safely set the default contentField for the parent class if missing
        boolean hasContentField =
                config.getOptional(
                                org.apache.seatunnel.api.configuration.Options.key("content_field")
                                        .stringType()
                                        .noDefaultValue())
                        .isPresent();

        if (!hasContentField) {
            this.contentField = HubSpotSourceParameter.DEFAULT_CONTENT_FIELD;
        }
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        // Use our custom parameter with the Auth headers instead of the parent's empty one
        return new HttpSourceReader(
                this.hubSpotSourceParameter,
                readerContext,
                this.deserializationSchema,
                jsonField,
                contentField);
    }

    @Override
    public String getPluginName() {
        return "HubSpot";
    }
}
