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
package org.apache.seatunnel.connectors.seatunnel.discourse;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
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

/**
 * Validates one bounded category or recent-topic read and exposes its resource-specific schema. The
 * source does not paginate topic history or maintain an incremental cursor.
 */
public class DiscourseSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    static final String PLUGIN_NAME = "Discourse";

    private final DiscourseSourceOptions.Resource resource;
    private final URI endpoint;
    private final String apiKey;
    private final String apiUsername;
    private final boolean includeSubcategories;
    private final CatalogTable table;

    /*
     * Validates paired HTTPS credentials and resource-specific options before creating the endpoint.
     * Appending the resource path preserves any deployment subpath in base_url.
     */
    public DiscourseSource(ReadonlyConfig options) {
        resource = options.get(DiscourseSourceOptions.RESOURCE);
        apiKey = options.getOptional(DiscourseSourceOptions.TOKEN).orElse(null);
        apiUsername = options.getOptional(DiscourseSourceOptions.USERNAME).orElse(null);
        if ((apiKey == null) != (apiUsername == null)) {
            throw new IllegalArgumentException(
                    "Discourse token and username must be configured together");
        }
        if (apiKey != null) {
            validateHeader(apiKey, "token");
            validateHeader(apiUsername, "username");
        }
        URI base = parseSite(options.get(DiscourseSourceOptions.BASE_URL));
        if (apiKey != null && !"https".equalsIgnoreCase(base.getScheme())) {
            throw new IllegalArgumentException("Discourse API authentication requires HTTPS");
        }

        String path;
        if (resource == DiscourseSourceOptions.Resource.CATEGORIES) {
            if (options.getOptional(DiscourseSourceOptions.TOPIC_LIMIT).isPresent()) {
                throw new IllegalArgumentException(
                        "Discourse topic_limit applies only to LATEST_TOPICS");
            }
            includeSubcategories = options.get(DiscourseSourceOptions.INCLUDE_SUBCATEGORIES);
            path = "/categories.json?include_subcategories=" + includeSubcategories;
        } else {
            if (options.getOptional(DiscourseSourceOptions.INCLUDE_SUBCATEGORIES).isPresent()) {
                throw new IllegalArgumentException(
                        "Discourse include_subcategories applies only to CATEGORIES");
            }
            int limit = options.get(DiscourseSourceOptions.TOPIC_LIMIT);
            if (limit < 1 || limit > 100) {
                throw new IllegalArgumentException(
                        "Discourse topic_limit must be between 1 and 100");
            }
            includeSubcategories = false;
            path = "/latest.json?per_page=" + limit;
        }
        endpoint = URI.create(base.toASCIIString().replaceAll("/+$", "") + path);
        table = createTable(resource);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    /*
     * Rejects streaming jobs because a single metadata response is a bounded batch, not a feed.
     */
    @Override
    public void setJobContext(JobContext context) {
        if (context.getJobMode() != JobMode.BATCH) {
            throw new IllegalArgumentException("Discourse source supports BATCH mode only");
        }
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(table);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext context) {
        return new DiscourseSourceReader(
                endpoint, apiKey, apiUsername, resource, includeSubcategories, table, context);
    }

    /*
     * Rejects empty or non-visible-ASCII credential headers without echoing their values.
     */
    private static void validateHeader(String value, String name) {
        if (value.isEmpty() || value.chars().anyMatch(c -> c < 0x21 || c > 0x7e)) {
            throw new IllegalArgumentException("Invalid Discourse " + name);
        }
    }

    /*
     * Accepts an HTTP(S) site root, preserving its subpath while rejecting URL credentials,
     * query strings, fragments and dot segments before an endpoint is appended.
     */
    private static URI parseSite(String url) {
        final URI site;
        try {
            site = URI.create(url);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Discourse base_url");
        }
        if (site.getHost() == null
                || (!"http".equalsIgnoreCase(site.getScheme())
                        && !"https".equalsIgnoreCase(site.getScheme()))
                || site.getRawUserInfo() != null
                || site.getRawQuery() != null
                || site.getRawFragment() != null
                || !site.normalize().equals(site)) {
            throw new IllegalArgumentException(
                    "Discourse base_url must be an HTTP(S) site URL without credentials, query, fragment or dot segments");
        }
        return site;
    }

    /*
     * Defines typed columns and nullability per resource. The primary key identifies a record
     * within this site and resource only; nested categories use the same category schema.
     */
    private static CatalogTable createTable(DiscourseSourceOptions.Resource resource) {
        TableSchema.Builder schema =
                TableSchema.builder()
                        .column(column("id", BasicType.LONG_TYPE, false))
                        .primaryKey(
                                PrimaryKey.of("pk_discourse_id", Collections.singletonList("id")));
        if (resource == DiscourseSourceOptions.Resource.CATEGORIES) {
            schema.column(column("name", BasicType.STRING_TYPE, false))
                    .column(column("slug", BasicType.STRING_TYPE, false))
                    .column(column("parent_category_id", BasicType.LONG_TYPE, true))
                    .column(column("topic_count", BasicType.LONG_TYPE, true))
                    .column(column("post_count", BasicType.LONG_TYPE, true));
        } else {
            schema.column(column("title", BasicType.STRING_TYPE, false))
                    .column(column("slug", BasicType.STRING_TYPE, false))
                    .column(column("category_id", BasicType.LONG_TYPE, true))
                    .column(column("created_at", BasicType.STRING_TYPE, true))
                    .column(column("last_posted_at", BasicType.STRING_TYPE, true))
                    .column(column("posts_count", BasicType.LONG_TYPE, true))
                    .column(column("views", BasicType.LONG_TYPE, true))
                    .column(column("closed", BasicType.BOOLEAN_TYPE, true))
                    .column(column("archived", BasicType.BOOLEAN_TYPE, true));
        }
        return CatalogTable.of(
                TableIdentifier.of(PLUGIN_NAME, null, null, resource.rows),
                schema.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private static PhysicalColumn column(String name, BasicType<?> type, boolean nullable) {
        return PhysicalColumn.of(name, type, 0, nullable, null, null);
    }
}
