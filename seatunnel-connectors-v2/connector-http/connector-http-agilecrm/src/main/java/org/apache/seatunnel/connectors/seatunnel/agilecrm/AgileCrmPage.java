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

package org.apache.seatunnel.connectors.seatunnel.agilecrm;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmClient.fail;

/** Validates the provider's record-list shape before it reaches the checkpoint boundary. */
final class AgileCrmPage {
    private static final int MAX_RECORDS = 10000;
    final List<SeaTunnelRow> rows = new ArrayList<>();
    final Set<Long> parents = new LinkedHashSet<>();
    private String next;

    static AgileCrmPage parse(
            AgileCrmSettings config,
            AgileCrmResource resource,
            Long parent,
            boolean discovery,
            String table,
            JsonNode response)
            throws AgileCrmClient.ReadFailure {
        AgileCrmPage page = new AgileCrmPage();
        List<JsonNode> values = records(resource, response);
        if (values.isEmpty()) {
            return page;
        }
        page.next = cursor(resource, values.get(values.size() - 1));
        if (resource.paging == AgileCrmResource.Paging.TICKET) {
            if (page.next != null || values.size() >= resource.requestedPageSize(config)) {
                throw fail(
                        resource,
                        "ticket filter returned a potentially incomplete page without supported continuation");
            }
        } else if (resource.paging == AgileCrmResource.Paging.NONE && page.next != null) {
            throw fail(resource, "unexpected continuation on a single-response resource");
        }

        for (JsonNode value : values) {
            if (!value.isObject()) {
                throw fail(resource, "list contains a value that is not an object");
            }
            JsonNode id = value.path("id");
            if (discovery) {
                page.parents.add(parentId(resource, id));
            } else {
                String recordId = id.isTextual() || id.isIntegralNumber() ? id.asText() : null;
                SeaTunnelRow output =
                        new SeaTunnelRow(
                                new Object[] {
                                    resource.name,
                                    recordId,
                                    parent == null ? null : parent.toString(),
                                    value.toString()
                                });
                output.setTableId(table);
                page.rows.add(output);
            }
        }
        return page;
    }

    String next() {
        return next;
    }

    private static List<JsonNode> records(AgileCrmResource resource, JsonNode response)
            throws AgileCrmClient.ReadFailure {
        if (response != null && response.isArray() && response.size() <= MAX_RECORDS) {
            List<JsonNode> values = new ArrayList<>(response.size());
            response.forEach(values::add);
            return values;
        }
        if (response != null
                && resource == AgileCrmResource.CAMPAIGNS
                && response.isObject()
                && !response.has("error")
                && (response.path("id").isTextual() || response.path("id").isIntegralNumber())) {
            return Collections.singletonList(response);
        }
        throw fail(resource, "response must be a JSON record list of at most 10000 entries");
    }

    private static Long parentId(AgileCrmResource resource, JsonNode id)
            throws AgileCrmClient.ReadFailure {
        if (id.isIntegralNumber() && id.canConvertToLong() && id.longValue() > 0L) {
            return id.longValue();
        }
        if (id.isTextual() && id.textValue().matches("[1-9][0-9]{0,18}")) {
            try {
                return Long.valueOf(id.textValue());
            } catch (NumberFormatException ignored) {
                // Out-of-range values get the same diagnostic as a malformed identifier.
            }
        }
        throw fail(resource, "parent id must be a positive signed 64-bit integer");
    }

    private static String cursor(AgileCrmResource resource, JsonNode row)
            throws AgileCrmClient.ReadFailure {
        JsonNode value = row.path("cursor");
        if (value.isMissingNode()
                || value.isNull()
                || (value.isTextual() && value.textValue().isEmpty())) {
            return null;
        }
        if (!value.isTextual() || value.textValue().length() > 2048) {
            throw fail(resource, "invalid last-record cursor");
        }
        return value.textValue();
    }
}
