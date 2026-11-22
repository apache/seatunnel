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

package org.apache.seatunnel.connectors.seatunnel.wordpress.source;

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

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

public class WordPressSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    public static final String PLUGIN_NAME = "WordPress";
    private static final Pattern HOSTNAME =
            Pattern.compile(
                    "[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+");

    private final String site;
    private final Instant start;
    private final Instant end;
    private final int pageSize;
    private final int maxPages;
    private final CatalogTable table;

    public WordPressSource(ReadonlyConfig options) {
        site = options.get(WordPressOptions.SITE);
        if (site == null
                || site.length() > 253
                || (!site.matches("[1-9][0-9]{0,18}") && !HOSTNAME.matcher(site).matches())) {
            throw new IllegalArgumentException(
                    "WordPress site must be a positive numeric ID or hostname");
        }
        start = parseDate(options.get(WordPressOptions.START_DATE), "start_date");
        end = parseDate(options.get(WordPressOptions.END_DATE), "end_date");
        if (start.isAfter(end)) {
            throw new IllegalArgumentException(
                    "WordPress start_date must be at or before end_date");
        }
        pageSize = options.get(WordPressOptions.PAGE_SIZE);
        maxPages = options.get(WordPressOptions.MAX_PAGES);
        if (pageSize < 1 || pageSize > 100 || maxPages < 1 || maxPages > 10000) {
            throw new IllegalArgumentException(
                    "WordPress page_size must be 1–100 and max_pages 1–10000");
        }
        table =
                CatalogTable.of(
                        TableIdentifier.of(PLUGIN_NAME, null, "posts"),
                        TableSchema.builder()
                                .column(column("id", BasicType.LONG_TYPE))
                                .column(column("slug", BasicType.STRING_TYPE))
                                .column(column("title", BasicType.STRING_TYPE))
                                .column(column("url", BasicType.STRING_TYPE))
                                .column(column("published_at", LocalTimeType.LOCAL_DATE_TIME_TYPE))
                                .column(column("modified_at", LocalTimeType.LOCAL_DATE_TIME_TYPE))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "Public WordPress.com posts");
    }

    private static Instant parseDate(String value, String option) {
        try {
            if (value != null
                    && value.matches(
                            "[0-9]{4}-[0-9]{2}-[0-9]{2}T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]Z")) {
                return Instant.parse(value);
            }
        } catch (DateTimeParseException ignored) {
            // Do not expose submitted option values.
        }
        throw new IllegalArgumentException(
                "WordPress " + option + " must be an ISO-8601 UTC instant with second precision");
    }

    private static PhysicalColumn column(String name, SeaTunnelDataType<?> type) {
        return PhysicalColumn.of(name, type, 0L, false, null, null);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void setJobContext(JobContext context) {
        if (context.getJobMode() != JobMode.BATCH) {
            throw new IllegalArgumentException("WordPress supports BATCH mode only");
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(table);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext context) {
        return new WordPressReader(site, start, end, pageSize, maxPages, table, context);
    }
}
