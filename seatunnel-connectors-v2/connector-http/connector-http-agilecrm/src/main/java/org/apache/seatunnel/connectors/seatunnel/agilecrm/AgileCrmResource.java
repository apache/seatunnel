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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Defines only the provider's list operations, including the read-only company POST. */
enum AgileCrmResource {
    CONTACTS("/contacts", Paging.CURSOR, null),
    COMPANIES("/contacts/companies/list", Paging.FORM, null),
    DEALS("/opportunity", Paging.CURSOR, null),
    NOTES("/contacts/%s/notes", Paging.NONE, CONTACTS),
    TASKS("/tasks", Paging.NONE, null),
    EVENTS("/events", Paging.NONE, null),
    MILESTONE("/milestone/pipelines", Paging.NONE, null),
    CAMPAIGNS("/workflows", Paging.CURSOR, null),
    DOCUMENTS("/documents/contact/%s/docs", Paging.NONE, CONTACTS),
    TICKET_FILTERS("/tickets/filters", Paging.NONE, null),
    TICKETS("/tickets/filter", Paging.TICKET, TICKET_FILTERS);

    enum Paging {
        NONE,
        CURSOR,
        FORM,
        TICKET
    }

    final String name;
    final Paging paging;
    final AgileCrmResource parent;
    private final String suffix;

    AgileCrmResource(String suffix, Paging paging, AgileCrmResource parent) {
        this.suffix = suffix;
        this.paging = paging;
        this.parent = parent;
        name = name().toLowerCase(Locale.ROOT);
    }

    static AgileCrmResource parse(String name) {
        for (AgileCrmResource candidate : values()) {
            if (candidate.name.equals(name)) {
                return candidate;
            }
        }
        throw AgileCrmSettings.invalid("resource is unsupported");
    }

    int requestedPageSize(AgileCrmSettings config) {
        return paging == Paging.TICKET ? Math.min(25, config.pageSize) : config.pageSize;
    }

    Request list(AgileCrmSettings config, Long parentId, String cursor) {
        String route =
                "/dev/api"
                        + (suffix.contains("%s")
                                ? String.format(Locale.ROOT, suffix, parentId)
                                : suffix);
        List<String> query = new ArrayList<>(4);
        if (paging != Paging.NONE) {
            query.add("page_size=" + requestedPageSize(config));
        }
        if (cursor != null && (paging == Paging.CURSOR || paging == Paging.FORM)) {
            query.add("cursor=" + escaped(cursor));
        }
        if (this == EVENTS) {
            query.add("start=" + config.eventStart);
            query.add("end=" + config.eventEnd);
        }
        if (this == COMPANIES) {
            query.add("global_sort_key=-created_time");
            return new Request(this, route, String.join("&", query));
        }
        if (this == TICKETS) {
            query.add("filter_id=" + parentId);
            query.add("global_sort_key=-last_updated_time");
        }
        return new Request(
                this, query.isEmpty() ? route : route + "?" + String.join("&", query), null);
    }

    private static String escaped(String text) {
        try {
            return URLEncoder.encode(text, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 is unavailable", e);
        }
    }

    static final class Request {
        final AgileCrmResource resource;
        final String target;
        final String form;

        private Request(AgileCrmResource resource, String target, String form) {
            this.resource = resource;
            this.target = target;
            this.form = form;
        }
    }
}
