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

package org.apache.seatunnel.connectors.seatunnel.opendatadc;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import java.net.URI;
import java.util.Collections;
import java.util.List;

public class OpenDataDcSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    static final String PLUGIN_NAME = "OpenDataDC";
    private final int pageSize;
    private final long maxRows;
    private final CatalogTable table;
    private final String queryUrl;

    OpenDataDcSource(ReadonlyConfig options) {
        pageSize = options.get(OpenDataDcSourceFactory.PAGE_SIZE);
        maxRows = options.get(OpenDataDcSourceFactory.MAX_ROWS);
        if (pageSize < 1 || pageSize > 1000 || maxRows < 0) {
            throw new IllegalArgumentException(
                    "page_size must be within 1..1000 and max_rows cannot be negative");
        }
        URI endpoint = URI.create(options.get(OpenDataDcSourceFactory.QUERY_URL));
        boolean allowedScheme =
                "https".equalsIgnoreCase(endpoint.getScheme())
                        || (options.get(OpenDataDcSourceFactory.ALLOW_INSECURE_HTTP)
                                && "http".equalsIgnoreCase(endpoint.getScheme()));
        if (!allowedScheme
                || endpoint.getHost() == null
                || endpoint.getRawUserInfo() != null
                || endpoint.getRawQuery() != null
                || endpoint.getRawFragment() != null
                || !endpoint.getPath().endsWith("/query")
                || endpoint.getPort() > 65535) {
            throw new IllegalArgumentException(
                    "query_url must be an HTTPS ArcGIS /query URL without credentials, query or fragment; HTTP requires allow_insecure_http");
        }
        queryUrl = endpoint.toASCIIString();
        TableSchema schema =
                TableSchema.builder()
                        .column(column("OBJECTID", BasicType.LONG_TYPE, false))
                        .column(column("CCN", BasicType.STRING_TYPE, true))
                        .column(column("REPORTDATE", LocalTimeType.LOCAL_DATE_TIME_TYPE, true))
                        .column(column("WARD", BasicType.STRING_TYPE, true))
                        .column(column("TOTAL_VEHICLES", BasicType.INT_TYPE, true))
                        .column(column("LATITUDE", BasicType.DOUBLE_TYPE, true))
                        .column(column("LONGITUDE", BasicType.DOUBLE_TYPE, true))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "crashes"),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "Open Data DC: Crashes in DC");
    }

    private static PhysicalColumn column(String name, SeaTunnelDataType<?> type, boolean nullable) {
        return PhysicalColumn.of(name, type, 0, nullable, null, null);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public void setJobContext(JobContext context) {
        if (context.getJobMode() != JobMode.BATCH) {
            throw new IllegalArgumentException("Open Data DC only supports BATCH jobs");
        }
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(table);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext context) {
        return new OpenDataDcReader(
                context,
                new CrashesApi(queryUrl),
                pageSize,
                maxRows,
                table.getTableId().toTablePath().getFullName());
    }
}
