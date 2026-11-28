/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.matomo;

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
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;

/**
 * Defines a bounded daily VisitsSummary report with a fixed five-column schema. Report dates use
 * the Matomo site timezone and restoration replays the configured range.
 */
public final class MatomoSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    static final String IDENTIFIER = "Matomo";

    private static final TableSchema SCHEMA =
            TableSchema.builder()
                    .column(PhysicalColumn.of("site_id", BasicType.INT_TYPE, 0, false, null, null))
                    .column(
                            PhysicalColumn.of(
                                    "report_date", BasicType.STRING_TYPE, 0, false, null, null))
                    .column(PhysicalColumn.of("visits", BasicType.LONG_TYPE, 0, false, null, null))
                    .column(PhysicalColumn.of("actions", BasicType.LONG_TYPE, 0, false, null, null))
                    .column(
                            PhysicalColumn.of(
                                    "unique_visitors", BasicType.LONG_TYPE, 0, true, null, null))
                    .build();

    private final URI endpoint;
    private final int siteId;
    private final LocalDate start;
    private final LocalDate end;
    private final String token;
    private final int timeoutMs;
    private JobContext jobContext;
    private final CatalogTable table =
            CatalogTable.of(
                    TableIdentifier.of(IDENTIFIER, TablePath.DEFAULT),
                    SCHEMA,
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    null);

    /**
     * Validates the endpoint, HTTPS token transport and an inclusive range of 1 to 366 days. Dates
     * retain the Matomo site timezone; option values are not echoed in validation errors.
     */
    public MatomoSource(ReadonlyConfig config) {
        token = config.getOptional(MatomoSourceOptions.TOKEN_AUTH).orElse(null);
        try {
            endpoint = URI.create(config.get(MatomoSourceOptions.URL));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Matomo url is not a valid HTTP(S) URL");
        }
        final String scheme = endpoint.getScheme();
        if (endpoint.getHost() == null
                || endpoint.getRawQuery() != null
                || endpoint.getRawFragment() != null
                || endpoint.getRawUserInfo() != null
                || !("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))) {
            throw new IllegalArgumentException(
                    "Matomo url must be HTTP(S), without user info, query, or fragment");
        }
        if (token != null && (token.isBlank() || !"https".equalsIgnoreCase(scheme))) {
            throw new IllegalArgumentException("Matomo token must be nonempty and requires HTTPS");
        }
        siteId = config.get(MatomoSourceOptions.SITE_ID);
        timeoutMs = config.get(MatomoSourceOptions.TIMEOUT_MS);
        if (siteId < 1 || timeoutMs < 1 || timeoutMs > 120000) {
            throw new IllegalArgumentException(
                    "Matomo requires a positive site_id and timeout_ms between 1 and 120000");
        }
        start = readDate(config.get(MatomoSourceOptions.START_DATE));
        end = readDate(config.get(MatomoSourceOptions.END_DATE));
        long days = ChronoUnit.DAYS.between(start, end);
        if (days < 0 || days >= 366) {
            throw new IllegalArgumentException("Matomo report range must contain 1 to 366 days");
        }
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    /*
     * Retains the engine mode so getBoundedness can reject streaming execution.
     */
    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    /*
     * Reports a finite batch and rejects a streaming context once supplied by the engine.
     */
    @Override
    public Boundedness getBoundedness() {
        if (jobContext != null && JobMode.STREAMING.equals(jobContext.getJobMode())) {
            throw new IllegalArgumentException("Matomo is a batch-only source");
        }
        return Boundedness.BOUNDED;
    }

    /**
     * Keeps catalog tables as the schema contract; the inherited legacy type adapter derives its
     * result from this same table without a redundant connector override.
     */
    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(table);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext context) {
        return new MatomoSourceReader(
                endpoint,
                siteId,
                start,
                end,
                token,
                timeoutMs,
                context,
                table.getTablePath().toString());
    }
    /*
     * Accepts calendar dates in YYYY-MM-DD form without exposing submitted values.
     */
    private static LocalDate readDate(String value) {
        try {
            if (value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
                return LocalDate.parse(value);
            }
        } catch (DateTimeException ignored) {
            // Do not put untrusted option values into logs.
        }
        throw new IllegalArgumentException("Matomo report dates must be valid YYYY-MM-DD values");
    }
}
