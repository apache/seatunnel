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

package org.apache.seatunnel.connectors.seatunnel.asana.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;

import java.util.HashMap;

/**
 * Builds the HTTP request parameters for the Asana tasks endpoint: the tasks URL, the Bearer
 * Authorization header, query params (project, page limit, fields, optional modified_since) and
 * retry defaults. Validates that api_key and project_gid are not blank.
 */
public class AsanaSourceParameter extends HttpParameter {

    private static String requireNonBlank(String value, String optionKey) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Asana option '%s' must not be blank.", optionKey));
        }
        return value.trim();
    }

    /**
     * Builds the Asana request: tasks URL, Bearer Authorization header, query params and retry
     * defaults. Validates that api_key and project_gid are not blank.
     */
    public void buildWithConfig(ReadonlyConfig config) {
        super.buildWithConfig(config);
        String apiKey = requireNonBlank(config.get(AsanaSourceOptions.API_KEY), "api_key");
        String projectGid =
                requireNonBlank(config.get(AsanaSourceOptions.PROJECT_GID), "project_gid");

        String base = config.get(AsanaSourceOptions.BASE_URL);
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        setUrl(base + "/tasks");
        setMethod(HttpRequestMethod.GET);

        if (headers == null) {
            headers = new HashMap<>();
        }
        headers.put("Authorization", "Bearer " + apiKey);

        params = new HashMap<>();
        params.put("project", projectGid);
        params.put("limit", "100");
        params.put(
                "opt_fields",
                "name,completed,completed_at,created_at,modified_at,due_on,"
                        + "assignee.gid,assignee.name,permalink_url");
        config.getOptional(AsanaSourceOptions.MODIFIED_SINCE)
                .ifPresent(v -> params.put("modified_since", v));
        setKeepPageParamAsHttpParam(true);
        setJsonFiledMissedReturnNull(true);

        if (!config.getOptional(AsanaSourceOptions.RETRY).isPresent()) {
            setRetry(3);
        }
        setRetryBackoffMultiplierMillis(config.get(AsanaSourceOptions.RETRY_BACKOFF_MULTIPLIER_MS));
        setRetryBackoffMaxMillis(config.get(AsanaSourceOptions.RETRY_BACKOFF_MAX_MS));

        if (getRetryBackoffMultiplierMillis() < 0 || getRetryBackoffMaxMillis() < 0) {
            throw new IllegalArgumentException("Asana retry backoff values must not be negative.");
        }
    }
}
