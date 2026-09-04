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

package org.apache.seatunnel.connectors.seatunnel.posthog.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.posthog.source.config.PostHogSourceParameter;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import java.util.Collections;
import java.util.List;

public class PostHogSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    public static final String PLUGIN_NAME = "PostHog";

    private final PostHogSourceParameter sourceParameter = new PostHogSourceParameter();
    private final CatalogTable catalogTable;
    private JobContext jobContext;

    public PostHogSource(ReadonlyConfig pluginConfig) {
        sourceParameter.buildWithConfig(pluginConfig);
        catalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Boundedness getBoundedness() {
        if (!JobMode.BATCH.equals(jobContext.getJobMode())) {
            throw new UnsupportedOperationException(
                    "PostHog source connector only supports batch mode");
        }
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) {
        return new PostHogSourceReader(
                sourceParameter,
                readerContext,
                new JsonDeserializationSchema(catalogTable, false, false),
                catalogTable.getSeaTunnelRowType());
    }
}
