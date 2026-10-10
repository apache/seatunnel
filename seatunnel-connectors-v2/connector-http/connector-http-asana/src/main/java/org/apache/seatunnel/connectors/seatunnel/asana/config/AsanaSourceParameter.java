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

import static org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions.DEFAULT_RETRY_BACKOFF_MAX_MS;
import static org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions.DEFAULT_RETRY_BACKOFF_MULTIPLIER_MS;

public class AsanaSourceParameter extends HttpParameter {

    /**
     * Overrides buildWithConfig to accept an explicit apiKey parameter. Asana's REST API requires
     * the API key to be passed specifically as an Authorization header, so this method ensures the
     * key is properly extracted and configured.
     */
    public void buildWithConfig(ReadonlyConfig config, String apiKey) {
        super.buildWithConfig(config);
        String base = config.get(AsanaSourceOptions.BASE_URL);
        if (base.endsWith("/")) base = base.substring(0, base.length() - 1);
        setUrl(base + "/tasks");
        setMethod(HttpRequestMethod.GET);
        if (headers == null) headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + apiKey);
        params = new HashMap<>();
        params.put("project", config.get(AsanaSourceOptions.PROJECT_GID));
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
            setRetryBackoffMultiplierMillis(DEFAULT_RETRY_BACKOFF_MULTIPLIER_MS);
            setRetryBackoffMaxMillis(DEFAULT_RETRY_BACKOFF_MAX_MS);
        }
    }
}
