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

package org.apache.seatunnel.connectors.seatunnel.stripe.source;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.stripe.source.config.StripeSourceParameter;

import java.util.Collections;
import java.util.List;

public class StripeSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    public static final String PLUGIN_NAME = "Stripe";

    private final StripeSourceParameter sourceParameter = new StripeSourceParameter();
    private final CatalogTable catalogTable;
    private JobContext jobContext;

    public StripeSource(ReadonlyConfig pluginConfig) {
        sourceParameter.buildWithConfig(pluginConfig);
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "content",
                                        BasicType.STRING_TYPE,
                                        0,
                                        false,
                                        null,
                                        "PaymentIntent object as JSON"))
                        .build();
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(PLUGIN_NAME, TablePath.DEFAULT),
                        tableSchema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);
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
        throw new UnsupportedOperationException("Stripe source only supports batch jobs");
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Lists.newArrayList(catalogTable);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) {
        return new StripeSourceReader(sourceParameter, readerContext);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }
}
