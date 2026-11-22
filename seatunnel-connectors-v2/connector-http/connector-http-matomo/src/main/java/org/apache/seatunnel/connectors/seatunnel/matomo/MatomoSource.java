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
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import java.net.URI;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;

public final class MatomoSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    static final String IDENTIFIER = "Matomo";
    static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "HTTP(S) Matomo Reporting API URL, including index.php when needed");
    static final Option<Integer> SITE_ID =
            Options.key("site_id").intType().noDefaultValue().withDescription("Matomo website ID");
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("First report date, inclusive, YYYY-MM-DD");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Last report date, inclusive, YYYY-MM-DD");
    static final Option<String> TOKEN_AUTH =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional private Matomo token; only sent in an HTTPS form body");
    static final Option<Integer> TIMEOUT_MS =
            Options.key("timeout_ms")
                    .intType()
                    .defaultValue(15000)
                    .withDescription("HTTP connect/read timeout, 1 to 120000 milliseconds");

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

    public MatomoSource(ReadonlyConfig config) {
        token = config.getOptional(TOKEN_AUTH).orElse(null);
        try {
            endpoint = URI.create(config.get(URL));
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
        siteId = config.get(SITE_ID);
        timeoutMs = config.get(TIMEOUT_MS);
        if (siteId < 1 || timeoutMs < 1 || timeoutMs > 120000) {
            throw new IllegalArgumentException(
                    "Matomo requires a positive site_id and timeout_ms between 1 and 120000");
        }
        start = readDate(config.get(START_DATE));
        end = readDate(config.get(END_DATE));
        long days = ChronoUnit.DAYS.between(start, end);
        if (days < 0 || days >= 366) {
            throw new IllegalArgumentException("Matomo report range must contain 1 to 366 days");
        }
    }

    private static LocalDate readDate(String value) {
        try {
            if (value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
                return LocalDate.parse(value);
            }
        } catch (java.time.DateTimeException ignored) {
            // Do not put untrusted option values into logs.
        }
        throw new IllegalArgumentException("Matomo report dates must be valid YYYY-MM-DD values");
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Boundedness getBoundedness() {
        if (jobContext != null && JobMode.STREAMING.equals(jobContext.getJobMode())) {
            throw new IllegalArgumentException("Matomo is a batch-only source");
        }
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return table.getSeaTunnelRowType();
    }

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
}
