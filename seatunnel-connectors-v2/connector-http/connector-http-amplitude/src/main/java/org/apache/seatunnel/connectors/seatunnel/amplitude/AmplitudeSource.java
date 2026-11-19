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
package org.apache.seatunnel.connectors.seatunnel.amplitude;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Bounded raw-event exports with whole-window replay on checkpoint restore. */
public class AmplitudeSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    private final AmplitudeSourceConfig config;
    private final CatalogTable table;

    public AmplitudeSource(ReadonlyConfig options) {
        config = new AmplitudeSourceConfig(options);
        table = CatalogTableUtil.buildWithConfig(options);
        for (String column : config.fieldMapping.keySet()) {
            if (!Arrays.asList(table.getSeaTunnelRowType().getFieldNames()).contains(column)) {
                throw AmplitudeSourceConfig.invalid("field_mapping keys must name schema columns");
            }
        }
    }

    @Override
    public String getPluginName() {
        return "Amplitude";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public void setJobContext(JobContext context) {
        if (context.getJobMode() != JobMode.BATCH) {
            throw AmplitudeSourceConfig.invalid("source supports BATCH only");
        }
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(table);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext context) {
        return new AmplitudeSourceReader(config, table, context);
    }
}
