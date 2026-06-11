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

package org.apache.seatunnel.connectors.seatunnel.airtable.source.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.google.common.base.Strings;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.airtable.config.AirtableConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AirtableSourceParameter extends HttpParameter {
    private static final String LIST_RECORDS_SUFFIX = "/listRecords";

    @Override
    public void buildWithConfig(ReadonlyConfig pluginConfig) {
        super.buildWithConfig(pluginConfig);
        String baseId = pluginConfig.get(AirtableSourceOptions.BASE_ID);
        String table = pluginConfig.get(AirtableSourceOptions.TABLE);
        String apiBaseUrl =
                pluginConfig
                        .getOptional(AirtableSourceOptions.API_BASE_URL)
                        .orElse(AirtableConfig.DEFAULT_API_BASE_URL);

        this.setUrl(AirtableConfig.buildBaseUrl(apiBaseUrl, baseId, table) + LIST_RECORDS_SUFFIX);
        this.setMethod(HttpRequestMethod.POST);

        String token = pluginConfig.get(AirtableSourceOptions.TOKEN);
        this.setHeaders(AirtableConfig.buildAuthHeaders(token, getHeaders()));

        this.setBody(buildRequestBody(pluginConfig, this.getBody()));
    }

    private String buildRequestBody(ReadonlyConfig pluginConfig, String existingBody) {
        Map<String, Object> body = new HashMap<>();
        if (!Strings.isNullOrEmpty(existingBody)) {
            try {
                Map<String, Object> parsed =
                        JsonUtils.parseObject(
                                existingBody, new TypeReference<Map<String, Object>>() {});
                if (parsed != null) {
                    body.putAll(parsed);
                }
            } catch (Exception ignored) {
                // Ignore non-JSON body and build Airtable request body from options.
            }
        }

        checkBodyConflicts(pluginConfig, body);

        pluginConfig
                .getOptional(AirtableSourceOptions.FIELDS)
                .ifPresent(value -> body.put("fields", value));
        pluginConfig
                .getOptional(AirtableSourceOptions.FILTER_BY_FORMULA)
                .ifPresent(value -> body.put("filterByFormula", value));
        pluginConfig
                .getOptional(AirtableSourceOptions.MAX_RECORDS)
                .ifPresent(value -> body.put("maxRecords", value));
        pluginConfig
                .getOptional(AirtableSourceOptions.PAGE_SIZE)
                .ifPresent(value -> body.put("pageSize", value));
        pluginConfig
                .getOptional(AirtableSourceOptions.SORT)
                .ifPresent(value -> body.put("sort", parseSort(value)));
        pluginConfig
                .getOptional(AirtableSourceOptions.VIEW)
                .ifPresent(value -> body.put("view", value));
        pluginConfig
                .getOptional(AirtableSourceOptions.CELL_FORMAT)
                .ifPresent(value -> body.put("cellFormat", value));
        pluginConfig
                .getOptional(AirtableSourceOptions.RETURN_FIELDS_BY_FIELD_ID)
                .ifPresent(value -> body.put("returnFieldsByFieldId", value));
        pluginConfig
                .getOptional(AirtableSourceOptions.RECORD_METADATA)
                .ifPresent(value -> body.put("recordMetadata", value));
        pluginConfig
                .getOptional(AirtableSourceOptions.TIME_ZONE)
                .ifPresent(value -> body.put("timeZone", value));
        pluginConfig
                .getOptional(AirtableSourceOptions.USER_LOCALE)
                .ifPresent(value -> body.put("userLocale", value));

        // Keep offset key for key-based cursor replacement in HttpSourceReader.
        // Dedicated option wins; otherwise preserve body offset if present.
        if (pluginConfig.getOptional(AirtableSourceOptions.OFFSET).isPresent()) {
            body.put("offset", pluginConfig.get(AirtableSourceOptions.OFFSET));
        } else {
            body.putIfAbsent("offset", null);
        }

        return JsonUtils.toJsonString(body);
    }

    private void checkBodyConflicts(ReadonlyConfig pluginConfig, Map<String, Object> body) {
        if (body.isEmpty()) {
            return;
        }
        List<String> conflicts = new ArrayList<>();
        checkConflict(pluginConfig, body, AirtableSourceOptions.FIELDS, "fields", conflicts);
        checkConflict(
                pluginConfig,
                body,
                AirtableSourceOptions.FILTER_BY_FORMULA,
                "filterByFormula",
                conflicts);
        checkConflict(
                pluginConfig, body, AirtableSourceOptions.MAX_RECORDS, "maxRecords", conflicts);
        checkConflict(pluginConfig, body, AirtableSourceOptions.PAGE_SIZE, "pageSize", conflicts);
        checkConflict(pluginConfig, body, AirtableSourceOptions.SORT, "sort", conflicts);
        checkConflict(pluginConfig, body, AirtableSourceOptions.VIEW, "view", conflicts);
        checkConflict(
                pluginConfig, body, AirtableSourceOptions.CELL_FORMAT, "cellFormat", conflicts);
        checkConflict(
                pluginConfig,
                body,
                AirtableSourceOptions.RETURN_FIELDS_BY_FIELD_ID,
                "returnFieldsByFieldId",
                conflicts);
        checkConflict(
                pluginConfig,
                body,
                AirtableSourceOptions.RECORD_METADATA,
                "recordMetadata",
                conflicts);
        checkConflict(pluginConfig, body, AirtableSourceOptions.TIME_ZONE, "timeZone", conflicts);
        checkConflict(
                pluginConfig, body, AirtableSourceOptions.USER_LOCALE, "userLocale", conflicts);
        checkConflict(pluginConfig, body, AirtableSourceOptions.OFFSET, "offset", conflicts);
        if (!conflicts.isEmpty()) {
            throw new IllegalArgumentException(
                    "Conflict between 'body' and dedicated Airtable options for keys: "
                            + String.join(", ", conflicts)
                            + ". Please use either the dedicated option or 'body', not both.");
        }
    }

    private void checkConflict(
            ReadonlyConfig pluginConfig,
            Map<String, Object> body,
            Option<?> option,
            String bodyKey,
            List<String> conflicts) {
        if (pluginConfig.getOptional(option).isPresent() && body.containsKey(bodyKey)) {
            conflicts.add(bodyKey + " (option: " + option.key() + ")");
        }
    }

    private Object parseSort(String sortJson) {
        try {
            return JsonUtils.parseObject(
                    sortJson, new TypeReference<List<Map<String, Object>>>() {});
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid sort JSON: " + sortJson, e);
        }
    }
}
