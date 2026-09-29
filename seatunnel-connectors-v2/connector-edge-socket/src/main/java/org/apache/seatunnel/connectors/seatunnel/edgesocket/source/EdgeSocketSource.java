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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketSourceOptions;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class EdgeSocketSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private final EdgeSocketConfig config;
    private final CatalogTable catalogTable;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private JobContext jobContext;

    public EdgeSocketSource(ReadonlyConfig readonlyConfig) {
        this.config = new EdgeSocketConfig(readonlyConfig);
        Optional<Map<String, Object>> schemaOptions =
                readonlyConfig.getOptional(ConnectorCommonOptions.SCHEMA);
        if (schemaOptions.isPresent()) {
            this.catalogTable = CatalogTableUtil.buildWithConfig(readonlyConfig);
            this.deserializationSchema = new JsonDeserializationSchema(catalogTable, false, false);
        } else {
            SeaTunnelRowType seaTunnelRowType =
                    new SeaTunnelRowType(
                            new String[] {"value"},
                            new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
            this.catalogTable =
                    CatalogTableUtil.getCatalogTable(
                            EdgeSocketSourceOptions.identifier, seaTunnelRowType);
            this.deserializationSchema =
                    new EdgeSocketTextDeserializationSchema(catalogTable.getSeaTunnelRowType());
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return EdgeSocketSourceOptions.identifier;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new EdgeSocketSourceReader(this.config, readerContext, this.deserializationSchema);
    }
}
