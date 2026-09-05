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

package org.apache.seatunnel.connectors.seatunnel.python.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import java.util.Collections;
import java.util.List;

/**
 * Source implementation that delegates record production to one Python process.
 *
 * <p>The current MVP is modeled as a bounded single-split source because one script execution
 * produces one finite stdout stream.
 */
public class PythonSource extends AbstractSingleSplitSource<SeaTunnelRow>
        implements SupportColumnProjection {

    private final PythonSourceConfig sourceConfig;
    private final CatalogTable catalogTable;

    public PythonSource(ReadonlyConfig pluginConfig) {
        this.sourceConfig = new PythonSourceConfig(pluginConfig);
        this.catalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public String getPluginName() {
        return PythonSourceOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) {
        return new PythonSourceReader(sourceConfig, catalogTable, readerContext);
    }
}
