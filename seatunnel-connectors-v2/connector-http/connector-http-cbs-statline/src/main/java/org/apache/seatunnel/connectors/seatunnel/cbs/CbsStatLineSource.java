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

package org.apache.seatunnel.connectors.seatunnel.cbs;

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

import java.net.URI;
import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.cbs.CbsStatLineSourceFactory.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.cbs.CbsStatLineSourceFactory.DATASET;
import static org.apache.seatunnel.connectors.seatunnel.cbs.CbsStatLineSourceFactory.IDENTIFIER;
import static org.apache.seatunnel.connectors.seatunnel.cbs.CbsStatLineSourceFactory.MAX_ROWS;

final class CbsStatLineSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    private final String dataset;
    private final URI observations;
    private final int maxRows;
    private final CatalogTable catalogTable;

    CbsStatLineSource(ReadonlyConfig config) {
        dataset = config.get(DATASET);
        maxRows = config.get(MAX_ROWS);
        observations = observationsUri(config.get(BASE_URL), dataset, maxRows);
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "dataset_id",
                                                BasicType.STRING_TYPE,
                                                0,
                                                false,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "id", BasicType.LONG_TYPE, 0, false, null, null))
                                .column(
                                        PhysicalColumn.of(
                                                "measure",
                                                BasicType.STRING_TYPE,
                                                0,
                                                false,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "value",
                                                BasicType.STRING_TYPE,
                                                0,
                                                true,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "string_value",
                                                BasicType.STRING_TYPE,
                                                0,
                                                true,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "value_attribute",
                                                BasicType.STRING_TYPE,
                                                0,
                                                true,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "observation",
                                                BasicType.STRING_TYPE,
                                                0,
                                                false,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);
    }

    static URI observationsUri(String baseUrl, String dataset, int maxRows) {
        if (dataset == null || !dataset.matches("[A-Za-z0-9_-]+")) {
            throw new IllegalArgumentException(
                    "dataset_id must contain only letters, digits, underscores or hyphens");
        }
        if (maxRows < 0) {
            throw new IllegalArgumentException("max_rows must be non-negative");
        }
        final URI origin;
        try {
            origin = URI.create(baseUrl);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("base_url must be a valid HTTP or HTTPS origin");
        }
        String host = origin.getHost();
        if (host == null
                || origin.getRawUserInfo() != null
                || origin.getRawQuery() != null
                || origin.getRawFragment() != null
                || origin.getPort() == 0
                || origin.getPort() > 65535
                || (!origin.getRawPath().isEmpty() && !"/".equals(origin.getRawPath()))
                || !("https".equalsIgnoreCase(origin.getScheme())
                        || "http".equalsIgnoreCase(origin.getScheme()))) {
            throw new IllegalArgumentException(
                    "base_url must be an HTTP or HTTPS origin without credentials, query, or path");
        }
        return origin.resolve(
                "/odata/v1/CBS/"
                        + dataset
                        + "/Observations"
                        + (maxRows == 0 ? "" : "?$top=" + maxRows));
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public void setJobContext(JobContext context) {
        if (context.getJobMode() != JobMode.BATCH) {
            throw new UnsupportedOperationException("CbsStatLine source supports only batch jobs");
        }
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
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext context) {
        return new CbsStatLineReader(observations, dataset, maxRows, context);
    }
}
