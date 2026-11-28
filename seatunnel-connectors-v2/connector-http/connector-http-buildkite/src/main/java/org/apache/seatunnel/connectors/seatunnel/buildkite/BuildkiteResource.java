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
package org.apache.seatunnel.connectors.seatunnel.buildkite;

import java.util.regex.Pattern;

/**
 * Declares native paths, parent relationships and response shapes for the Buildkite source.
 *
 * <p>Only the build lists receive creation bounds on the wire. Parent discovery stays independent
 * of the time filters used for emitted rows.
 */
enum BuildkiteResource {
    ORGANIZATIONS("organizations", "/v2/organizations", null, Shape.PAGED, true, false),
    SUITES(
            "analytics_organizations_suites",
            "/v2/analytics/organizations/%s/suites",
            ORGANIZATIONS,
            Shape.SUITES,
            false,
            false),
    PIPELINES(
            "organizations_pipelines",
            "/v2/organizations/%s/pipelines",
            ORGANIZATIONS,
            Shape.PAGED,
            true,
            false),
    ACCESS_TOKEN("access-token", "/v2/access-token", null, Shape.SINGLE, false, false),
    BUILDS("builds", "/v2/builds", null, Shape.PAGED, true, true),
    CLUSTERS(
            "organizations_clusters",
            "/v2/organizations/%s/clusters",
            ORGANIZATIONS,
            Shape.PAGED,
            true,
            false),
    ORGANIZATION_BUILDS(
            "organizations_builds",
            "/v2/organizations/%s/builds",
            ORGANIZATIONS,
            Shape.PAGED,
            true,
            true),
    PIPELINE_BUILDS(
            "organizations_pipelines_builds",
            "/v2/organizations/%s/pipelines/%s/builds",
            PIPELINES,
            Shape.PAGED,
            true,
            true),
    QUEUES(
            "organizations_clusters_queues",
            "/v2/organizations/%s/clusters/%s/queues",
            CLUSTERS,
            Shape.PAGED,
            true,
            false),
    CLUSTER_TOKENS(
            "organizations_clusters_tokens",
            "/v2/organizations/%s/clusters/%s/tokens",
            CLUSTERS,
            Shape.PAGED,
            true,
            false),
    EMOJIS(
            "organizations_emojis",
            "/v2/organizations/%s/emojis",
            ORGANIZATIONS,
            Shape.ARRAY,
            false,
            false),
    USER("user", "/v2/user", null, Shape.SINGLE, true, false);

    private static final Pattern PATH_KEY = Pattern.compile("[A-Za-z0-9_.-]{1,200}");

    final String name;
    private final String template;
    final BuildkiteResource parent;
    final Shape shape;
    final boolean dated;
    final boolean wireDates;

    BuildkiteResource(
            String name,
            String template,
            BuildkiteResource parent,
            Shape shape,
            boolean dated,
            boolean wireDates) {
        this.name = name;
        this.template = template;
        this.parent = parent;
        this.shape = shape;
        this.dated = dated;
        this.wireDates = wireDates;
    }

    String path(String org, String partition) {
        return String.format(template, org, partition);
    }

    String identity() {
        if (this == ACCESS_TOKEN) {
            return "uuid";
        }
        return this == EMOJIS ? "name" : "id";
    }

    boolean hasCredentialMetadata() {
        return this == ACCESS_TOKEN || this == SUITES || this == CLUSTER_TOKENS;
    }

    static boolean pathKey(String key) {
        return key != null
                && !".".equals(key)
                && !"..".equals(key)
                && PATH_KEY.matcher(key).matches();
    }

    Failure failure(String reason) {
        return new Failure(name, reason);
    }

    /**
     * Separates paged lists, singleton metadata and full-list responses.
     *
     * <p>Suites accept an audited next relation for deployments that paginate the catalog.
     */
    enum Shape {
        PAGED,
        SUITES,
        ARRAY,
        SINGLE
    }

    /**
     * Carries a bounded diagnostic tied to the selected resource.
     *
     * <p>Provider payloads, query parameters, and credentials are omitted.
     */
    static final class Failure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failure(String name, String reason) {
            super("Buildkite " + name + ": " + reason);
        }
    }
}
