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

package org.apache.seatunnel.connectors.seatunnel.sonarqube;

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

import org.apache.http.client.utils.URIBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.sonarqube.SonarQubeSourceFactory.IDENTIFIER;
import static org.apache.seatunnel.connectors.seatunnel.sonarqube.SonarQubeSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.sonarqube.SonarQubeSourceOptions.METRIC_KEYS;
import static org.apache.seatunnel.connectors.seatunnel.sonarqube.SonarQubeSourceOptions.PROJECT_KEY;
import static org.apache.seatunnel.connectors.seatunnel.sonarqube.SonarQubeSourceOptions.TOKEN_ENV;

/**
 * Validates one bounded main-branch project-measures read and its fixed output schema. The source
 * accepts no historical or incremental cursor and supports one reader.
 */
final class SonarQubeSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    private final String projectKey;
    private final Set<String> metricKeys;
    private final String tokenEnv;
    private final URI endpoint;
    private final CatalogTable table;

    SonarQubeSource(ReadonlyConfig config) {
        projectKey = config.get(PROJECT_KEY);
        metricKeys = validateMetrics(config.get(METRIC_KEYS));
        tokenEnv = config.getOptional(TOKEN_ENV).orElse(null);
        endpoint = measuresUri(config.get(BASE_URL), projectKey, metricKeys, tokenEnv);
        table =
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "project_key",
                                                BasicType.STRING_TYPE,
                                                0,
                                                false,
                                                null,
                                                "SonarQube project key"))
                                .column(
                                        PhysicalColumn.of(
                                                "project_name",
                                                BasicType.STRING_TYPE,
                                                0,
                                                false,
                                                null,
                                                "SonarQube project name"))
                                .column(
                                        PhysicalColumn.of(
                                                "metric_key",
                                                BasicType.STRING_TYPE,
                                                0,
                                                false,
                                                null,
                                                "SonarQube metric key"))
                                .column(
                                        PhysicalColumn.of(
                                                "value",
                                                BasicType.STRING_TYPE,
                                                0,
                                                true,
                                                null,
                                                "Current overall metric value, if returned"))
                                .column(
                                        PhysicalColumn.of(
                                                "best_value",
                                                BasicType.BOOLEAN_TYPE,
                                                0,
                                                true,
                                                null,
                                                "Whether the current value is the optimum, if returned"))
                                .column(
                                        PhysicalColumn.of(
                                                "measure",
                                                BasicType.STRING_TYPE,
                                                0,
                                                false,
                                                null,
                                                "Compact SonarQube measure JSON, including period fields"))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);
    }

    /*
     * Preserves requested metric order while rejecting duplicates, invalid names or more than 50 keys.
     * Missing returned metrics are allowed; this validates the request, not response completeness.
     */
    static Set<String> validateMetrics(List<String> metrics) {
        if (metrics == null || metrics.isEmpty() || metrics.size() > 50) {
            throw new IllegalArgumentException(
                    "metric_keys must contain 1 to 50 distinct metric keys");
        }
        LinkedHashSet<String> values = new LinkedHashSet<>();
        for (String metric : metrics) {
            if (metric == null || !metric.matches("[A-Za-z0-9_]{1,128}") || !values.add(metric)) {
                throw new IllegalArgumentException(
                        "metric_keys must contain unique alphanumeric or underscore keys");
            }
        }
        return values;
    }

    /*
     * Validates a project, optional worker token-variable name and normalized server context path.
     * Authenticated requests require HTTPS; the token itself is resolved only by the worker reader.
     */
    static URI measuresUri(String baseUrl, String projectKey, Set<String> keys, String tokenEnv) {
        if (projectKey == null || !projectKey.matches("[A-Za-z0-9_.:-]{1,400}")) {
            throw new IllegalArgumentException("project_key must be a valid SonarQube project key");
        }
        if (tokenEnv != null && !tokenEnv.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("token_env must be an environment variable name");
        }
        try {
            URI base = new URI(baseUrl);
            boolean https = "https".equalsIgnoreCase(base.getScheme());
            if ((!https && !"http".equalsIgnoreCase(base.getScheme()))
                    || base.getHost() == null
                    || base.getRawUserInfo() != null
                    || base.getRawQuery() != null
                    || base.getRawFragment() != null
                    || base.getPort() == 0
                    || base.getPort() > 65535
                    || !base.getRawPath().matches("(?:/[A-Za-z0-9._~-]+)*/?")
                    || !base.getRawPath().equals(base.normalize().getRawPath())) {
                throw new IllegalArgumentException(
                        "base_url must be an HTTP(S) server URL with an optional context path");
            }
            if (tokenEnv != null && !https) {
                throw new IllegalArgumentException(
                        "SonarQube bearer authentication requires HTTPS");
            }
            return new URIBuilder(base)
                    .setPath(base.getPath().replaceFirst("/$", "") + "/api/measures/component")
                    .addParameter("component", projectKey)
                    .addParameter("metricKeys", String.join(",", keys))
                    .build();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("base_url must be a valid SonarQube URL");
        }
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    /*
     * Rejects streaming mode because current project measures are fetched once per batch.
     */
    @Override
    public void setJobContext(JobContext context) {
        if (context.getJobMode() != JobMode.BATCH) {
            throw new UnsupportedOperationException("SonarQube source supports only batch jobs");
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
        return new SonarQubeSourceReader(endpoint, projectKey, metricKeys, tokenEnv, context);
    }
}
