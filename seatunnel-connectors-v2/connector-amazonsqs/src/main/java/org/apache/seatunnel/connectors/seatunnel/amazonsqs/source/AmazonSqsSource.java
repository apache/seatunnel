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

package org.apache.seatunnel.connectors.seatunnel.amazonsqs.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

@Slf4j
public class AmazonSqsSource extends AbstractSingleSplitSource<SeaTunnelRow>
        implements SupportColumnProjection {

    private AmazonSqsSourceConfig amazonSqsSourceConfig;
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private CatalogTable catalogTable;

    public AmazonSqsSource(
            AmazonSqsSourceConfig amazonSqsSourceConfig,
            CatalogTable catalogTable,
            DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.amazonSqsSourceConfig = amazonSqsSourceConfig;
        this.catalogTable = catalogTable;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public String getPluginName() {
        return "AmazonSqs";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new AmazonSqsSourceReader(
                readerContext,
                amazonSqsSourceConfig,
                deserializationSchema,
                catalogTable.getSeaTunnelRowType());
    }
}
